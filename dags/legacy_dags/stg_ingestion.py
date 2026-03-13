"""Stg ingestion DAG — load parquet per table, run dbt, test, trigger silver.

Accepts conf: {"source_name": "postgres_crm"} to process a single source.
If no conf is provided, processes all sources.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_source_configs
from src.ingestion.stg import run_stg_subject

CONFIG_PATH = Path("/opt/airflow/config/src2brz_config.csv")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BASE_URL = "/opt/airflow/data/bronze"
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")


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


def load_subject_tables(source_name: str, source_schema: str, data_subject: str, **kwargs):
    sources = load_source_configs(CONFIG_PATH)
    source_config = next(s for s in sources if s.name == source_name)
    tables = [t for t in source_config.tables if t.data_subject == data_subject]
    credentials = _load_warehouse_credentials()

    run_stg_subject(
        source_name=source_name,
        source_schema=source_schema,
        data_subject=data_subject,
        tables=tables,
        bronze_base_url=BRONZE_BASE_URL,
        warehouse_credentials=credentials,
    )


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


def run_dbt_test_stg(**kwargs):
    credentials = _load_warehouse_credentials()
    _set_dbt_env_vars(credentials)
    result = subprocess.run(
        ["dbt", "test", "--select", "stg", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt test failed with exit code {result.returncode}")


with DAG(
    dag_id="stg_ingestion",
    description="Load latest parquet into stg, run dbt stg models and tests",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["stg", "ingestion", "dbt"],
) as dag:
    sources = load_source_configs(CONFIG_PATH) if CONFIG_PATH.exists() else []

    SENSOR_DEADLINE = 120  # 120s

    def _latest_ingestion_run(logical_date, **kwargs):
        from airflow.models import DagRun # type: ignore
        runs = (
            DagRun.find(dag_id="src2brz_rdbms2parquet_ingestion", state="success")
            + DagRun.find(dag_id="src2brz_rdbms2parquet_ingestion", state="running")
        )
        if runs:
            runs.sort(key=lambda r: r.execution_date, reverse=True)
            return runs[0].execution_date
        return logical_date

    wait_ingestion = ExternalTaskSensor(
        task_id="wait_ingestion",
        external_dag_id="src2brz_rdbms2parquet_ingestion",
        external_task_id=None,
        mode="reschedule",
        timeout=SENSOR_DEADLINE,
        poke_interval=60,
        soft_fail=True,
        execution_date_fn=_latest_ingestion_run,
    )

    # One load task per source + data_subject
    load_tasks = []
    seen = set()
    for source in sources:
        for table in source.tables:
            key = (source.name, table.data_subject)
            if key in seen:
                continue
            seen.add(key)
            task = PythonOperator(
                task_id=f"load_{source.name}__{table.data_subject}",
                python_callable=load_subject_tables,
                op_kwargs={
                    "source_name": source.name,
                    "source_schema": source.schema,
                    "data_subject": table.data_subject,
                },
            )
            wait_ingestion >> task
            load_tasks.append(task)

    dbt_run = PythonOperator(
        task_id="dbt_run_stg",
        python_callable=run_dbt_stg,
        trigger_rule="all_done",
    )

    dbt_test = PythonOperator(
        task_id="dbt_test_stg",
        python_callable=run_dbt_test_stg,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="silver_transform",
    )

    for task in load_tasks:
        task >> dbt_run
    dbt_run >> dbt_test >> trigger_silver
