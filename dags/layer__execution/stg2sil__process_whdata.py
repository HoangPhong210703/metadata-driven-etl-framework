"""Execution DAG (stg2sil) — runs dbt models for data cleaning and standardization.
Task chain: run_dbt_stg → run_dbt_snapshot → run_dbt_silver → test_dbt."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.datasets import Dataset  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.utils.audit import audited
from src.utils.alert import dag_failure_callback, dag_success_callback

SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")


def _load_warehouse_credentials() -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def _setup_dbt_env():
    """Load warehouse credentials and set dbt env vars."""
    from src.cli.stg_cli import set_dbt_env_vars
    credentials = _load_warehouse_credentials()
    print(f"DEBUG CREDENTIALS: {credentials}")
    set_dbt_env_vars(credentials)


@audited
def write_dag_run_note(**kwargs):
    """Record which dataset events triggered this stg2sil run."""
    from airflow.settings import Session  # type: ignore

    triggering_events = kwargs.get("triggering_dataset_events", {})
    datasets = sorted(str(ds) for ds in triggering_events.keys()) if triggering_events else []
    note_text = f"Triggered by datasets: {', '.join(datasets)}" if datasets else "Triggered manually"

    session = Session()
    try:
        dr = session.merge(kwargs["dag_run"])
        dr.note = note_text
        session.commit()
    finally:
        session.close()

    print(f"[stg2sil] {note_text}")


@audited
def run_dbt_stg(**kwargs):
    """Run dbt staging models."""
    from src.cli.stg_cli import run_dbt
    _setup_dbt_env()
    run_dbt(DBT_PROJECT_DIR, selectors=["stg"])
    print("[stg2sil] dbt stg models complete")


@audited
def run_dbt_snapshot(**kwargs):
    """Run dbt snapshots (SCD-2 dimensions)."""
    from src.cli.stg_cli import run_dbt_snapshot
    _setup_dbt_env()
    run_dbt_snapshot(DBT_PROJECT_DIR)
    print("[stg2sil] dbt snapshots complete")


@audited
def run_dbt_silver(**kwargs):
    """Run dbt silver models (facts + dimensions)."""
    from src.cli.stg_cli import run_dbt
    _setup_dbt_env()
    run_dbt(DBT_PROJECT_DIR, selectors=["silver"])
    print("[stg2sil] dbt silver models complete")


@audited
def test_dbt(**kwargs):
    """Run dbt tests and store results to meta.dbt_test_results."""
    from src.cli.stg_cli import run_dbt_test, parse_dbt_results
    from src.utils.audit.db_logger import log_dbt_results

    _setup_dbt_env()
    run_dbt_test(DBT_PROJECT_DIR)

    results = parse_dbt_results(DBT_PROJECT_DIR)
    run_id = kwargs["dag_run"].run_id if kwargs.get("dag_run") else "unknown"

    try:
        log_dbt_results(results, run_id)
    except Exception as e:
        print(f"[stg2sil] WARNING: Failed to log dbt results to DB: {e}")

    passed = [r for r in results if r["status"] == "pass"]
    failed = [r for r in results if r["status"] in ("fail", "error")]
    warned = [r for r in results if r["status"] == "warn"]

    print(f"[stg2sil] dbt tests: {len(passed)} passed, {len(failed)} failed, {len(warned)} warnings")

    return {
        "test_count": len(results),
        "passed": len(passed),
        "failed": len(failed),
        "warnings": len(warned),
    }


with DAG(
    dag_id="stg2sil__process_whdata",
    description="Run dbt models for data cleaning and standardization (stg → snapshot → silver → test)",
    schedule=[
        Dataset("stg__accounting__postgres_crm"),
        Dataset("stg__accounting__postgres_timesheet"),
    ],
    start_date=datetime(2024, 1, 1),
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
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

    write_note = PythonOperator(
        task_id="write_dag_run_note",
        python_callable=write_dag_run_note,
    )

    write_note >> dbt_stg >> dbt_snap >> dbt_silver >> dbt_test
