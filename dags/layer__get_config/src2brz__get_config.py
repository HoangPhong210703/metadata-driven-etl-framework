"""Get config DAG — reads CSV config for a specific (data_subject, source) pair
and triggers processing."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

CONFIG_FILES = {
    "src2brz": Path("/opt/airflow/config/src2brz_config.csv"),
}


@audited
def get_config(**kwargs):
    """Read the layer config CSV, filter by the single data_subject + source from coordinator."""
    from src.ingestion.config import load_csv_config, get_active_tables

    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}
    layer = conf.get("layer", "src2brz")
    data_subject = conf.get("data_subject")
    source = conf.get("source")

    csv_path = CONFIG_FILES.get(layer)
    if not csv_path or not csv_path.exists():
        raise FileNotFoundError(f"Config file not found for layer '{layer}': {csv_path}")

    all_configs = load_csv_config(csv_path)
    active = get_active_tables(all_configs)

    # Filter to this specific (data_subject, source) pair
    if data_subject:
        active = [c for c in active if c.data_subject == data_subject]
    if source:
        active = [c for c in active if c.source_name == source]

    tables = [
        {
            "id": c.id,
            "table_name": c.table_name,
            "table_schema_stg": c.table_schema_stg,
            "source_name": c.source_name,
            "source_schema": c.source_schema,
            "data_subject": c.data_subject,
            "load_strategy": c.load_strategy,
            "cursor_column": c.cursor_column,
            "initial_value": c.initial_value,
            "primary_key": c.primary_key,
            "load_sequence": c.load_sequence,
        }
        for c in active
    ]

    print(f"[get_config] layer={layer}, data_subject={data_subject}, source={source}")
    print(f"[get_config] Active tables: {len(tables)}")

    return {
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables,
    }


with DAG(
    dag_id="src2brz_get_config",
    description="Read config for a (data_subject, source) pair and trigger processing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    get_config_task = PythonOperator(
        task_id="get_config",
        python_callable=get_config,
    )

    processing_trigger = TriggerDagRunOperator(
        task_id="processing_trigger",
        trigger_dag_id="src2brz_processing",
        conf="{{ ti.xcom_pull(task_ids='get_config') }}",
    )

    get_config_task >> processing_trigger  # type: ignore
