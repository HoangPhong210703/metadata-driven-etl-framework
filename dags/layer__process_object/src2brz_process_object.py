"""Process object DAG — receives config for a single (data_subject, source) pair,
sorts tables by load_sequence, and triggers the ingestion DAG."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited


@audited
def process_object(**kwargs):
    """Sort tables by load_sequence and prepare payload for ingestion."""
    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}

    layer = conf.get("layer", "src2brz")
    data_subject = conf.get("data_subject")
    source = conf.get("source")
    tables = conf.get("tables", [])

    # Sort by load_sequence ascending
    tables_sorted = sorted(tables, key=lambda t: int(t.get("load_sequence", 0)))

    print(f"[process_object] layer={layer}, data_subject={data_subject}, source={source}")
    print(f"[process_object] Tables ({len(tables_sorted)}):")
    for t in tables_sorted:
        print(f"  seq={t['load_sequence']} {t['table_name']}")

    return {
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables_sorted,
    }


with DAG(
    dag_id="src2brz_processing",
    description="Sort tables by load_sequence and trigger ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    process_object_task = PythonOperator(
        task_id="process_object",
        python_callable=process_object,
    )

    ingest_trigger = TriggerDagRunOperator(
        task_id="ingest_trigger",
        trigger_dag_id="src2brz_rdbms2parquet_ingestion",
        conf="{{ ti.xcom_pull(task_ids='process_object') }}",
    )

    process_object_task >> ingest_trigger  # type: ignore
