"""Ingestion DAG (rdbms2parquet) — for each (source, data_subject) group:
rdbms_src_connect → fetch_tables → write_parquet.

Tasks are created at parse time from src2brz_config.csv. At runtime, the conf
from the processing DAG determines which groups actually execute."""

import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_csv_config, get_active_tables, csv_to_source_configs, TableConfig, SourceConfig

CONFIG_PATH = Path("/opt/airflow/config/src2brz_config.csv")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"


def _load_credentials(source_name: str) -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["sources"][source_name]["credentials"]


def rdbms_src_connect(source_name: str, source_schema: str, data_subject: str, **kwargs):
    """Check database connectivity for this source."""
    # Skip if this group isn't in the conf's active list
    dag_run = kwargs.get("dag_run")
    if dag_run and dag_run.conf:
        active_subjects = dag_run.conf.get("data_subjects", [])
        if active_subjects and data_subject not in active_subjects:
            from airflow.exceptions import AirflowSkipException # type: ignore
            raise AirflowSkipException(f"Data subject '{data_subject}' not in active list")
        
        # Skip if source filter is specified and doesn't match
        source_filter = dag_run.conf.get("source")
        if source_filter and source_name != source_filter:
            from airflow.exceptions import AirflowSkipException # type: ignore
            raise AirflowSkipException(f"Source '{source_name}' does not match filter '{source_filter}'")

    from src.ingestion.bronze import test_source_connection
    credentials = _load_credentials(source_name)
    test_source_connection(credentials, source_schema)


def fetch_tables(source_name: str, source_schema: str, data_subject: str, **kwargs):
    """Extract data from RDBMS and normalize using table metadata from runtime config."""
    from src.ingestion.bronze import extract_tables
    from src.ingestion.config import SourceConfig, TableConfig

    # Get table metadata from runtime config passed from processing DAG
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    tables_list = conf.get("tables", [])
    
    # Filter to tables for this group
    tables_for_group = [
        t for t in tables_list
        if t.get("source_name") == source_name
        and t.get("data_subject") == data_subject
    ]
    
    # Fall back to CSV if no runtime config
    if not tables_for_group:
        configs = load_csv_config(CONFIG_PATH)
        csv_configs = [
            c for c in configs
            if c.source_name == source_name
            and c.data_subject == data_subject
            and c.table_load_active
        ]
        csv_configs.sort(key=lambda c: c.load_sequence)
        source_config = csv_to_source_configs(csv_configs)[0]
    else:
        # Build SourceConfig from runtime table metadata
        table_configs = [
            TableConfig(
                name=t["table_name"],
                load_strategy=t.get("load_strategy", "full"),
                data_subject=t.get("data_subject", ""),
                layer_subject_source=t.get("layer_subject_source", ""),
                cursor_column=t.get("cursor_column"),
                initial_value=t.get("initial_value"),
                primary_key=[t.get("primary_key")] if t.get("primary_key") else None,
            )
            for t in tables_for_group
        ]
        source_config = SourceConfig(
            name=source_name,
            schema=source_schema,
            tables=table_configs,
        )

    credentials = _load_credentials(source_name)
    extract_tables(source_config, BUCKET_URL, credentials, data_subject)


def write_parquet(source_name: str, source_schema: str, data_subject: str, **kwargs):
    """Write normalized data to parquet files using table metadata from runtime config."""
    from src.ingestion.bronze import load_to_parquet
    from src.ingestion.config import SourceConfig, TableConfig

    # Get table metadata from runtime config passed from processing DAG
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    tables_list = conf.get("tables", [])
    
    # Filter to tables for this group
    tables_for_group = [
        t for t in tables_list
        if t.get("source_name") == source_name
        and t.get("data_subject") == data_subject
    ]
    
    # Fall back to CSV if no runtime config
    if not tables_for_group:
        configs = load_csv_config(CONFIG_PATH)
        csv_configs = [
            c for c in configs
            if c.source_name == source_name
            and c.data_subject == data_subject
            and c.table_load_active
        ]
        csv_configs.sort(key=lambda c: c.load_sequence)
        source_config = csv_to_source_configs(csv_configs)[0]
    else:
        # Build SourceConfig from runtime table metadata
        table_configs = [
            TableConfig(
                name=t["table_name"],
                load_strategy=t.get("load_strategy", "full"),
                data_subject=t.get("data_subject", ""),
                layer_subject_source=t.get("layer_subject_source", ""),
                cursor_column=t.get("cursor_column"),
                initial_value=t.get("initial_value"),
                primary_key=[t.get("primary_key")] if t.get("primary_key") else None,
            )
            for t in tables_for_group
        ]
        source_config = SourceConfig(
            name=source_name,
            schema=source_schema,
            tables=table_configs,
        )

    load_to_parquet(source_config, BUCKET_URL, data_subject)


# --- Build task chains at parse time from CSV ---
_configs = load_csv_config(CONFIG_PATH) if CONFIG_PATH.exists() else []
_active = get_active_tables(_configs)

# Group by (source_name, data_subject)
_groups: dict[tuple[str, str, str], list] = {}
for c in _active:
    key = (c.source_name, c.source_schema, c.data_subject)
    if key not in _groups:
        _groups[key] = []
    _groups[key].append(c)

with DAG(
    dag_id="src2brz_rdbms2parquet_ingestion",
    description="RDBMS to parquet — per (source, data_subject) task chains",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "rdbms2parquet"],
) as dag:
    for (src_name, src_schema, ds), tables in sorted(_groups.items()):
        group_id = f"{src_name}__{ds}"
        op_kwargs = {
            "source_name": src_name,
            "source_schema": src_schema,
            "data_subject": ds,
        }

        connect = PythonOperator(
            task_id=f"{group_id}__rdbms_src_connect",
            python_callable=rdbms_src_connect,
            op_kwargs=op_kwargs,
        )

        fetch = PythonOperator(
            task_id=f"{group_id}__fetch_tables",
            python_callable=fetch_tables,
            op_kwargs=op_kwargs,
        )

        write = PythonOperator(
            task_id=f"{group_id}__write_parquet",
            python_callable=write_parquet,
            op_kwargs=op_kwargs,
        )

        connect >> fetch >> write # type: ignore
