"""Ingestion DAG (rdbms2parquet) — receives a single (data_subject, source) payload
from process_object, then runs: rdbms_src_connect → fetch_tables → write_parquet."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"


def _load_credentials(source_name: str) -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["sources"][source_name]["credentials"]


@audited
def rdbms_src_connect(**kwargs):
    """Check database connectivity for this source."""
    from src.ingestion.bronze import test_source_connection

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    tables = conf.get("tables", [])
    source_schema = tables[0]["source_schema"] if tables else "public"

    credentials = _load_credentials(source)
    test_source_connection(credentials, source_schema)
    print(f"[ingestion] Connected to {source} (schema: {source_schema})")


@audited
def fetch_tables(**kwargs):
    """Extract data from RDBMS and normalize."""
    from src.ingestion.bronze import extract_tables
    from src.ingestion.config import csv_to_source_configs, CsvTableConfig

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])

    # Rebuild CsvTableConfig objects from the conf dicts
    table_configs = [
        CsvTableConfig(
            id=t["id"],
            table_name=t["table_name"],
            table_schema_stg=t["table_schema_stg"],
            source_name=t["source_name"],
            source_schema=t["source_schema"],
            data_subject=t["data_subject"],
            load_strategy=t["load_strategy"],
            cursor_column=t["cursor_column"],
            initial_value=t["initial_value"],
            primary_key=t["primary_key"],
            load_sequence=t["load_sequence"],
            table_load_active=True,
        )
        for t in tables
    ]

    source_config = csv_to_source_configs(table_configs)[0]
    credentials = _load_credentials(source)
    extract_tables(source_config, BUCKET_URL, credentials, data_subject)
    print(f"[ingestion] Extracted {len(tables)} tables for {source}__{data_subject}")


@audited
def write_parquet(**kwargs):
    """Write normalized data to parquet files."""
    from src.ingestion.bronze import load_to_parquet
    from src.ingestion.config import SourceConfig

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])
    source_schema = tables[0]["source_schema"] if tables else "public"

    load_to_parquet(
        SourceConfig(name=source, schema=source_schema, tables=[]),
        BUCKET_URL,
        data_subject,
    )
    print(f"[ingestion] Wrote parquet for {source}__{data_subject}")


with DAG(
    dag_id="src2brz_rdbms2parquet_ingestion",
    description="RDBMS to parquet for a single (data_subject, source)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["ingestion", "rdbms2parquet"],
) as dag:
    connect = PythonOperator(
        task_id="rdbms_src_connect",
        python_callable=rdbms_src_connect,
    )

    fetch = PythonOperator(
        task_id="fetch_tables",
        python_callable=fetch_tables,
    )

    write = PythonOperator(
        task_id="write_parquet",
        python_callable=write_parquet,
    )

    trigger_brz2stg = TriggerDagRunOperator(
        task_id="trigger_brz2stg",
        trigger_dag_id="coordinator",
        conf={"button": "brz2stg__{{ dag_run.conf['data_subject'] }}__{{ dag_run.conf['source'] }}"},
        wait_for_completion=False,
    )

    connect >> fetch >> write >> trigger_brz2stg  # type: ignore
