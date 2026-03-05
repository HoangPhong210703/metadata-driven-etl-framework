"""Stg ingestion DAG — loads last N days of bronze parquet into postgres, runs dbt."""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_sources_config
from src.ingestion.stg import run_stg_ingestion

CONFIG_PATH = Path("/opt/airflow/config/sources.yaml")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BASE_URL = "/opt/airflow/data/bronze"
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
RETENTION_DAYS = 7


def _load_warehouse_credentials() -> str:
    import tomllib

    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def _set_dbt_env_vars(credentials: str) -> None:
    from urllib.parse import urlparse

    parsed = urlparse(credentials)
    os.environ["WAREHOUSE_HOST"] = parsed.hostname or "localhost"
    os.environ["WAREHOUSE_PORT"] = str(parsed.port or 5432)
    os.environ["WAREHOUSE_USER"] = parsed.username or ""
    os.environ["WAREHOUSE_PASSWORD"] = parsed.password or ""
    os.environ["WAREHOUSE_DB"] = (parsed.path or "").lstrip("/")


def load_parquet_to_stg_temp(source_name: str, **kwargs):
    sources = load_sources_config(CONFIG_PATH)
    credentials = _load_warehouse_credentials()
    source_config = next(s for s in sources if s.name == source_name)
    run_stg_ingestion(source_config, BRONZE_BASE_URL, credentials, RETENTION_DAYS)


def run_dbt_stg(**kwargs):
    credentials = _load_warehouse_credentials()
    _set_dbt_env_vars(credentials)
    result = subprocess.run(
        ["dbt", "run", "--select", "stg", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")


with DAG(
    dag_id="stg_ingestion",
    description="Load last 7 days of bronze parquet into stg_temp, run dbt stg models",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["stg", "ingestion", "dbt"],
) as dag:
    sources = load_sources_config(CONFIG_PATH) if CONFIG_PATH.exists() else []

    load_tasks = []
    for source in sources:
        task = PythonOperator(
            task_id=f"load_{source.name}_to_stg_temp",
            python_callable=load_parquet_to_stg_temp,
            op_kwargs={"source_name": source.name},
        )
        load_tasks.append(task)

    dbt_task = PythonOperator(
        task_id="dbt_run_stg",
        python_callable=run_dbt_stg,
    )

    for load_task in load_tasks:
        load_task >> dbt_task
