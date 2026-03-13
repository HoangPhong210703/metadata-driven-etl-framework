"""Bronze ingestion DAG for postgres_crm source."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_source_configs
from src.ingestion.bronze import rotate_todays_parquet, run_data_subject_ingestion, test_source_connection

CONFIG_PATH = Path("/opt/airflow/config/src2brz_config.csv")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"
SOURCE_NAME = "postgres_crm"


def _load_credentials() -> str:
    import tomllib

    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["sources"][SOURCE_NAME]["credentials"]


def rotate(**kwargs):
    sources = load_source_configs(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == SOURCE_NAME)
    rotate_todays_parquet(BUCKET_URL, source_config)


def source_connection(**kwargs):
    sources = load_source_configs(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == SOURCE_NAME)
    credentials = _load_credentials()
    test_source_connection(credentials, source_config.schema)


def fetch_data_subject(data_subject: str, **kwargs):
    sources = load_source_configs(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == SOURCE_NAME)
    credentials = _load_credentials()
    run_data_subject_ingestion(source_config, BUCKET_URL, credentials, data_subject)


with DAG(
    dag_id="bronze_postgres_crm",
    description="Bronze ingestion from postgres_crm",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["bronze", "postgres_crm"],
) as dag:
    load_credentials = PythonOperator(task_id="load_credentials", python_callable=_load_credentials)
    #rotate_task = PythonOperator(task_id="rotate_parquet", python_callable=rotate)
    source_connection_task = PythonOperator(task_id="source_connection", python_callable=source_connection)

    sources = load_source_configs(CONFIG_PATH) if CONFIG_PATH.exists() else []
    source_config = next((s for s in sources if s.name == SOURCE_NAME), None)

    if source_config:
        data_subjects = {t.data_subject for t in source_config.tables}
        for ds in sorted(data_subjects):
            task = PythonOperator(
                task_id=f"fetch__{ds}",
                python_callable=fetch_data_subject,
                op_kwargs={"data_subject": ds},
            )
            source_connection_task >> task

    load_credentials >> source_connection_task
