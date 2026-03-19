"""Execution DAG (stg2sil) — runs dbt models for data cleaning and standardization.
Task chain: run_dbt_stg → run_dbt_snapshot → run_dbt_silver → test_dbt."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")


def _load_warehouse_credentials() -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def _setup_dbt_env():
    """Load warehouse credentials and set dbt env vars."""
    from src.ingestion.stg_cli import set_dbt_env_vars
    credentials = _load_warehouse_credentials()
    set_dbt_env_vars(credentials)


@audited
def run_dbt_stg(**kwargs):
    """Run dbt staging models."""
    from src.ingestion.stg_cli import run_dbt
    _setup_dbt_env()
    run_dbt(DBT_PROJECT_DIR, selectors=["stg"])
    print("[stg2sil] dbt stg models complete")


@audited
def run_dbt_snapshot(**kwargs):
    """Run dbt snapshots (SCD-2 dimensions)."""
    from src.ingestion.stg_cli import run_dbt_snapshot
    _setup_dbt_env()
    run_dbt_snapshot(DBT_PROJECT_DIR)
    print("[stg2sil] dbt snapshots complete")


@audited
def run_dbt_silver(**kwargs):
    """Run dbt silver models (facts + dimensions)."""
    from src.ingestion.stg_cli import run_dbt
    _setup_dbt_env()
    run_dbt(DBT_PROJECT_DIR, selectors=["silver"])
    print("[stg2sil] dbt silver models complete")


@audited
def test_dbt(**kwargs):
    """Run dbt tests. Non-blocking — logs warnings on failure."""
    from src.ingestion.stg_cli import run_dbt_test
    _setup_dbt_env()
    run_dbt_test(DBT_PROJECT_DIR)
    print("[stg2sil] dbt tests complete")


with DAG(
    dag_id="stg2sil__process_whdata",
    description="Run dbt models for data cleaning and standardization (stg → snapshot → silver → test)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["execution", "stg2sil", "dbt"],
) as dag:
    dbt_stg = PythonOperator(
        task_id="run_dbt_stg",
        python_callable=run_dbt_stg,
    )

    dbt_snap = PythonOperator(
        task_id="run_dbt_snapshot",
        python_callable=run_dbt_snapshot,
    )

    dbt_silver = PythonOperator(
        task_id="run_dbt_silver",
        python_callable=run_dbt_silver,
    )

    dbt_test = PythonOperator(
        task_id="test_dbt",
        python_callable=test_dbt,
    )

    dbt_stg >> dbt_snap >> dbt_silver >> dbt_test  # type: ignore
