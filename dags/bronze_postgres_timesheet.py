"""Bronze ingestion DAG for postgres_timesheet source."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_sources_config
from src.ingestion.bronze import rotate_todays_parquet, run_source_ingestion

CONFIG_PATH = Path("/opt/airflow/config/sources.yaml")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"
SOURCE_NAME = "postgres_timesheet"


def _load_credentials() -> str:
    import tomllib

    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["sources"][SOURCE_NAME]["credentials"]


def rotate(**kwargs):
    sources = load_sources_config(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == SOURCE_NAME)
    rotate_todays_parquet(BUCKET_URL, source_config)


def ingest(**kwargs):
    sources = load_sources_config(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == SOURCE_NAME)
    credentials = _load_credentials()
    run_source_ingestion(source_config, BUCKET_URL, credentials)


with DAG(
    dag_id="bronze_postgres_timesheet",
    description="Bronze ingestion from postgres_timesheet",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["bronze", "postgres_timesheet"],
) as dag:
    load_credentials = PythonOperator(task_id="load_credentials", python_callable=_load_credentials)
    #rotate_task = PythonOperator(task_id="rotate_parquet", python_callable=rotate)
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)
    trigger_stg = TriggerDagRunOperator(
        task_id="trigger_stg",
        trigger_dag_id="stg_ingestion",
        conf={"source_name": SOURCE_NAME},
    )

    load_credentials >> ingest_task >> trigger_stg
