"""Silver transform DAG — snapshot, dim, fact as separate tasks with dbt test."""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")


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


def _run_dbt(command: list[str]) -> None:
    credentials = _load_warehouse_credentials()
    _set_dbt_env_vars(credentials)
    result = subprocess.run(
        ["dbt"] + command + ["--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


def run_snapshot_dim_customer(**kwargs):
    _run_dbt(["snapshot", "--select", "silver__customer__dim_customer"])


def run_dim_date(**kwargs):
    _run_dbt(["run", "--select", "silver__common__dim_date"])


def run_fact_lead(**kwargs):
    _run_dbt(["run", "--select", "silver__crm__fact_lead"])


def run_dbt_test_silver(**kwargs):
    _run_dbt(["test", "--select", "silver"])


with DAG(
    dag_id="silver_transform",
    description="Run dbt snapshot, dim, fact models and tests",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["silver", "dbt"],
) as dag:
    snapshot_task = PythonOperator(
        task_id="run_silver__customer__dim_customer",
        python_callable=run_snapshot_dim_customer,
    )

    dim_date_task = PythonOperator(
        task_id="run_silver__common__dim_date",
        python_callable=run_dim_date,
    )

    fact_lead_task = PythonOperator(
        task_id="run_silver__crm__fact_lead",
        python_callable=run_fact_lead,
    )

    test_task = PythonOperator(
        task_id="dbt_test_silver",
        python_callable=run_dbt_test_silver,
    )

    # dim_date has no upstream dependency, runs in parallel with snapshot
    # fact_lead depends on both dim_customer (snapshot) and dim_date
    [snapshot_task, dim_date_task] >> fact_lead_task >> test_task
