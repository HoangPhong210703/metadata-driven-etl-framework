"""Bronze ingestion DAG — one task per source, daily schedule."""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to path so we can import src modules
sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_sources_config
from src.ingestion.bronze import run_source_ingestion

CONFIG_PATH = Path("/opt/airflow/config/sources.yaml")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"


def _load_secrets() -> dict[str, str]:
    import tomllib

    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)

    secrets = {}
    for key, value in raw.get("sources", {}).items():
        if isinstance(value, dict) and "credentials" in value:
            secrets[key] = value["credentials"]
    return secrets


def ingest_source(source_name: str, **kwargs):
    """Callable for each PythonOperator — ingests a single source."""
    sources = load_sources_config(CONFIG_PATH)
    secrets = _load_secrets()

    source_config = next(s for s in sources if s.name == source_name)
    credentials = secrets[source_name]

    run_source_ingestion(source_config, BUCKET_URL, credentials)


with DAG(
    dag_id="bronze_ingestion",
    description="Daily bronze layer ingestion from source databases to parquet",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:
    sources = load_sources_config(CONFIG_PATH) if CONFIG_PATH.exists() else []

    for source in sources:
        PythonOperator(
            task_id=f"ingest_{source.name}",
            python_callable=ingest_source,
            op_kwargs={"source_name": source.name},
        )
