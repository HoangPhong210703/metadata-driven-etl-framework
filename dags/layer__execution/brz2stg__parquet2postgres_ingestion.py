"""Ingestion DAG (parquet2postgres) — receives a single (data_subject, source) payload
from process_object, then runs: verify_parquet → load_to_warehouse."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BASE_URL = "/opt/airflow/data/bronze"


def _load_warehouse_credentials() -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


@audited
def verify_parquet(**kwargs):
    """Check that parquet files exist for the tables in this (data_subject, source) pair."""
    from src.ingestion.stg import get_parquet_dir, get_latest_parquet_file

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])

    found = 0
    missing = 0
    for t in tables:
        parquet_dir = get_parquet_dir(
            bronze_base_url=BRONZE_BASE_URL,
            data_subject=data_subject,
            source_name=source,
            schema=t["source_schema"],
            table_name=t["table_name"],
        )
        latest = get_latest_parquet_file(parquet_dir)
        if latest:
            found += 1
        else:
            missing += 1
            print(f"[verify_parquet] No parquet found: {parquet_dir}")

    print(f"[verify_parquet] {source}__{data_subject}: {found} found, {missing} missing out of {len(tables)} tables")

    if found == 0:
        raise FileNotFoundError(f"No parquet files found for any table in {source}__{data_subject}")


@audited
def load_to_warehouse(**kwargs):
    """Load latest parquet files into warehouse using stg.py."""
    from src.ingestion.stg import run_stg_subject, _print_stg_summary
    from src.ingestion.config import TableConfig

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])

    table_configs = [
        TableConfig(
            name=t["table_name"],
            load_strategy=t["load_strategy"],
            data_subject=t["data_subject"],
            cursor_column=t.get("cursor_column") or None,
            initial_value=t.get("initial_value") or None,
            primary_key=[t["primary_key"]] if t.get("primary_key") else None,
        )
        for t in tables
    ]

    source_schema = tables[0]["source_schema"] if tables else "public"
    warehouse_credentials = _load_warehouse_credentials()

    results = run_stg_subject(
        source_name=source,
        source_schema=source_schema,
        data_subject=data_subject,
        tables=table_configs,
        bronze_base_url=BRONZE_BASE_URL,
        warehouse_credentials=warehouse_credentials,
    )

    _print_stg_summary(source, results)

    failed = [r for r in results if r[1] == "failed"]
    if failed:
        raise RuntimeError(f"{len(failed)} table(s) failed to load: {[r[0] for r in failed]}")


with DAG(
    dag_id="brz2stg_parquet2postgres_ingestion",
    description="Load parquet files into postgres warehouse for a single (data_subject, source)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "brz2stg", "parquet2postgres"],
) as dag:
    verify = PythonOperator(
        task_id="verify_parquet",
        python_callable=verify_parquet,
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    verify >> load  # type: ignore
