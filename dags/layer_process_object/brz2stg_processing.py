"""Processing DAG for brz2stg layer."""

from datetime import datetime
from collections import defaultdict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def read_config(**kwargs):
    """Read the config passed from the get_config DAG."""
    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}
    tables = conf.get("tables", [])
    layer = conf.get("layer", "brz2stg")
    data_subjects = conf.get("data_subjects", [])

    print(f"[brz2stg_processing] Layer: {layer}")
    print(f"[brz2stg_processing] Data subjects: {data_subjects}")
    print(f"[brz2stg_processing] Tables to process: {len(tables)}")

    return conf


def sequence_loads(**kwargs):
    """Group tables by layer_subject_source."""
    ti = kwargs["ti"]
    conf = ti.xcom_pull(task_ids="read_config")
    tables = conf.get("tables", [])
    source_filter = conf.get("source")

    groups = defaultdict(list)
    for t in tables:
        key = t['layer_subject_source']
        groups[key].append(t)

    sequenced = {
        "layer": conf.get("layer", "brz2stg"),
        "data_subjects": conf.get("data_subjects", []),
        "groups": {k: v for k, v in groups.items()},
        "tables": tables,
    }
    
    if source_filter:
        sequenced["source"] = source_filter

    for key, group_tables in groups.items():
        table_names = [t["table_name"] for t in group_tables]
        print(f"[brz2stg_processing] {key}: {table_names}")

    return sequenced


with DAG(
    dag_id="brz2stg_processing",
    description="Sequence table loads and trigger brz2stg ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration", "brz2stg"],
) as dag:
    read_config_task = PythonOperator(
        task_id="read_config",
        python_callable=read_config,
    )

    sequence_loads_task = PythonOperator(
        task_id="sequence_loads",
        python_callable=sequence_loads,
    )

    ingest_trigger = TriggerDagRunOperator(
        task_id="ingest_trigger",
        trigger_dag_id="brz2stg_parquet2postgresql_ingestion",
        conf="{{ ti.xcom_pull(task_ids='sequence_loads') }}",
    )

    read_config_task >> sequence_loads_task >> ingest_trigger
