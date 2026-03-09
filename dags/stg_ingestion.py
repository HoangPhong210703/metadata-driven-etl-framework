"""Stg ingestion DAG — load parquet per table, run dbt, test, trigger silver.

Accepts conf: {"source_name": "postgres_crm"} to process a single source.
If no conf is provided, processes all sources.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_sources_config
from src.ingestion.stg import (
    build_stg_pipeline,
    get_parquet_dir,
    get_recent_parquet_files,
)

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


def _get_source_name(kwargs) -> str | None:
    dag_run = kwargs.get("dag_run")
    if dag_run and dag_run.conf:
        return dag_run.conf.get("source_name")
    return None


def load_table_to_stg_temp(source_name: str, table_name: str, **kwargs):
    from dlt.sources.filesystem import readers

    sources = load_sources_config(CONFIG_PATH)
    credentials = _load_warehouse_credentials()
    source_config = next(s for s in sources if s.name == source_name)
    table_config = next(t for t in source_config.tables if t.name == table_name)

    pipeline = build_stg_pipeline(source_config, credentials)

    parquet_dir = get_parquet_dir(
        bronze_base_url=BRONZE_BASE_URL,
        data_subject=table_config.data_subject,
        source_name=source_config.name,
        schema=source_config.schema,
        table_name=table_config.name,
    )

    recent_files = get_recent_parquet_files(parquet_dir, RETENTION_DAYS)

    if not recent_files:
        print(f"[stg] No recent parquet files for {table_config.name}, skipping")
        return

    stg_temp_name = f"temp_{table_config.data_subject}_{source_config.name}_{table_config.name}"
    for i, parquet_file in enumerate(recent_files):
        disposition = "replace" if i == 0 else "append"
        reader = readers(
            bucket_url=str(parquet_file.parent),
            file_glob=parquet_file.name,
        ).read_parquet()
        load_info = pipeline.run(
            reader.with_name(stg_temp_name),
            write_disposition=disposition,
        )
        print(f"[stg] Loaded {parquet_file.name} → {stg_temp_name} ({disposition}): {load_info}")


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
    description="Load parquet per table into stg_temp, run dbt stg models and tests",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["stg", "ingestion", "dbt"],
) as dag:
    from collections import defaultdict
    from airflow.utils.task_group import TaskGroup

    sources = load_sources_config(CONFIG_PATH) if CONFIG_PATH.exists() else []

    # Group tables by data_subject
    tables_by_subject: dict[str, list[tuple]] = defaultdict(list)
    for source in sources:
        for table in source.tables:
            tables_by_subject[table.data_subject].append((source, table))

    # Create a TaskGroup per data_subject
    group_tasks = []
    for data_subject, entries in tables_by_subject.items():
        with TaskGroup(group_id=data_subject) as group:
            for source, table in entries:
                PythonOperator(
                    task_id=f"load_temp_{source.name}_{table.name}",
                    python_callable=load_table_to_stg_temp,
                    op_kwargs={"source_name": source.name, "table_name": table.name},
                )
        group_tasks.append(group)

    dbt_run = PythonOperator(
        task_id="dbt_run_stg",
        python_callable=run_dbt_stg,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test_stg",
        python_callable=run_dbt_test_stg,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="silver_transform",
    )

    group_tasks >> dbt_run >> dbt_test >> trigger_silver
