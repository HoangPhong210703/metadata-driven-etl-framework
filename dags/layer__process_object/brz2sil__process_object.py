"""Process object DAG (brz2sil) — receives config for a single (data_subject, source) pair,
filters active tables, sorts by load_sequence, and triggers the parquet-to-postgres ingestion DAG."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.utils.audit import audited


@audited
def process_object(**kwargs):
    """Filter active tables, sort by load_sequence, and prepare payload for ingestion."""
    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}

    layer = conf.get("layer", "brz2sil")
    data_subject = conf.get("data_subject")
    source = conf.get("source")
    tables = conf.get("tables", [])

    # Filter active tables, then sort by load_sequence
    tables_active = [t for t in tables if t.get("table_load_active", True)]
    tables_sorted = sorted(tables_active, key=lambda t: int(t.get("load_sequence", 0)))

    print(f"[process_object] layer={layer}, data_subject={data_subject}, source={source}")
    print(f"[process_object] Tables: {len(tables)} total, {len(tables_active)} active")
    for t in tables_sorted:
        print(f"  seq={t['load_sequence']} {t['table_name']}")

    return {
        "button": conf.get("button"),
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables_sorted,
    }


with DAG(
    dag_id="brz2sil__process_object",
    description="Filter active tables, sort by load_sequence, and trigger brz2stg ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration", "brz2sil"],
) as dag:
    process_object_task = PythonOperator(
        task_id="process_object",
        python_callable=process_object,
    )

    ingest_trigger = TriggerDagRunOperator(
        task_id="ingest_trigger",
        trigger_dag_id="brz2stg_parquet2postgres_ingestion",
        conf="{{ ti.xcom_pull(task_ids='process_object') }}",
    )

    process_object_task >> ingest_trigger  # type: ignore
