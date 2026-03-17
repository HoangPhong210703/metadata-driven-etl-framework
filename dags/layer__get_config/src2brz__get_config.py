"""Get config DAG — reads the appropriate config file based on the layer,
filters by active data subjects, and triggers processing."""

import json
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")

CONFIG_FILES = {
    "src2brz": Path("/opt/airflow/config/src2brz_config.csv"),
}


def get_config(**kwargs):
    """Read the layer config CSV, filter by active data subjects and optional source from coordinator."""
    from src.ingestion.config import load_csv_config, get_active_tables

    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}
    layer = conf.get("layer", "src2brz")
    active_subjects = conf.get("data_subjects", [])
    source_filter = conf.get("source")  # Optional source filter

    csv_path = CONFIG_FILES.get(layer)
    if not csv_path or not csv_path.exists():
        raise FileNotFoundError(f"Config file not found for layer '{layer}': {csv_path}")

    all_configs = load_csv_config(csv_path)
    active = get_active_tables(all_configs)

    # Filter to only the data subjects the coordinator approved
    if active_subjects:
        active = [c for c in active if c.data_subject in active_subjects]
    
    # Filter to specific source if provided
    if source_filter:
        active = [c for c in active if c.source_name == source_filter]

    tables_info = [
        {
            "id": c.id,
            "layer__data_subject__src": c.layer__data_subject__src,
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

    print(f"[get_config] Layer: {layer}")
    if source_filter:
        print(f"[get_config] Source filter: {source_filter}")
    print(f"[get_config] Active tables: {len(tables_info)} "
          f"across subjects: {sorted(set(c['data_subject'] for c in tables_info))}")

    result = {"layer": layer, "data_subjects": active_subjects, "tables": tables_info}
    if source_filter:
        result["source"] = source_filter
    return result


with DAG(
    dag_id="src2brz_get_config",
    description="Read layer config file and trigger processing",
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

    get_config_task >> processing_trigger # type: ignore