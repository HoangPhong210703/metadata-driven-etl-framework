"""Processing DAG — reads config, sequences table loads by load_sequence,
and triggers the ingestion DAG."""

import json
import sys
from collections import defaultdict
from datetime import datetime

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

sys.path.insert(0, "/opt/airflow")


def read_config(**kwargs):
    """Read the config passed from get_config DAG."""
    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}
    tables = conf.get("tables", [])
    layer = conf.get("layer", "src2brz")
    data_subjects = conf.get("data_subjects", [])

    print(f"[processing] Layer: {layer}")
    print(f"[processing] Data subjects: {data_subjects}")
    print(f"[processing] Tables to process: {len(tables)}")

    return conf


def sequence_loads(**kwargs):
    """Sequence tables by load_sequence (ascending). Group by (source, data_subject).
    Tables with same load_sequence can run in parallel."""
    ti = kwargs["ti"]
    conf = ti.xcom_pull(task_ids="read_config")
    tables = conf.get("tables", [])

    # Tables are already sorted by load_sequence from get_config.
    # Group by (source_name, data_subject) for the ingestion DAG.
    groups = defaultdict(list)
    for t in tables:
        key = f"{t['source_name']}__{t['data_subject']}"
        groups[key].append(t)

    sequenced = {
        "layer": conf.get("layer", "src2brz"),
        "data_subjects": conf.get("data_subjects", []),
        "groups": {k: v for k, v in groups.items()},
    }

    for key, group_tables in groups.items():
        table_names = [t["table_name"] for t in group_tables]
        print(f"[processing] {key}: {table_names}")

    return sequenced


with DAG(
    dag_id="brz2stg_processing",
    description="Sequence table loads and trigger ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
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
        trigger_dag_id="src2brz_rdbms2parquet_ingestion",
        conf="{{ ti.xcom_pull(task_ids='sequence_loads') }}",
    )

    read_config_task >> sequence_loads_task >> ingest_trigger
