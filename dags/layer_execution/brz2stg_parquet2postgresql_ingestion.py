"""Ingestion DAG (parquet2postgresql) — for each source config group:
load parquet files to staging postgresql."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_source_configs, csv_to_source_configs, CsvTableConfig, load_csv_config

CONFIG_PATH = Path("/opt/airflow/config/brz2stg_config.csv")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BUCKET_URL = "/opt/airflow/data/bronze"


def _load_warehouse_credentials() -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def stg_ingest(layer_subject_source: str, **kwargs):
    """Loads data from bronze parquet files into the staging warehouse."""
    from src.ingestion.stg import run_stg_ingestion
    from src.ingestion.config import CsvTableConfig, SourceConfig, TableConfig

    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    tables_list = conf.get("tables", [])
    
    tables_for_group = [t for t in tables_list if t.get("layer_subject_source") == layer_subject_source]

    if not tables_for_group:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException(f"No tables found for group {layer_subject_source} in runtime conf.")

    # Reconstruct a SourceConfig object for this specific group
    source_name = tables_for_group[0]["source_name"]
    source_schema = tables_for_group[0]["source_schema"]
    table_configs = [
        TableConfig(
            name=t["table_name"],
            load_strategy="", # Not needed for stg
            data_subject=t["data_subject"],
            layer_subject_source=t["layer_subject_source"],
        ) for t in tables_for_group
    ]
    source_config = SourceConfig(name=source_name, schema=source_schema, tables=table_configs)

    warehouse_credentials = _load_warehouse_credentials()
    
    print(f"[brz2stg_ingestion] Running for {layer_subject_source}")
    run_stg_ingestion(source_config, BRONZE_BUCKET_URL, warehouse_credentials)


# --- Build task chains at parse time from CSV ---
_configs = load_csv_config(CONFIG_PATH) if CONFIG_PATH.exists() else []
_layer_subject_sources = sorted(list(set(c.layer_subject_source for c in _configs if c.table_load_active)))


with DAG(
    dag_id="brz2stg_parquet2postgresql_ingestion",
    description="Parquet to PostgreSQL — one task per layer_subject_source",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "brz2stg", "parquet2postgresql"],
) as dag:
    for lss in _layer_subject_sources:
        
        # Define the task for this group
        ingest_task = PythonOperator(
            task_id=f"ingest_{lss}",
            python_callable=stg_ingest,
            op_kwargs={"layer_subject_source": lss},
        )

        # No dependencies between these tasks, they can run in parallel if resources allow
