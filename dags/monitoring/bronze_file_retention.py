"""Monitoring DAG — cleans up bronze parquet files older than the configured retention window.
Runs daily at 2 AM UTC. Reads retention_config.csv for per-source retention days."""

import csv
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

RETENTION_CONFIG_PATH = Path("/opt/airflow/config/retention_config.csv")


@audited
def run_retention(**kwargs):
    """Read retention_config.csv and delete expired parquet files per source."""
    from src.ingestion.retention import cleanup_source

    if not RETENTION_CONFIG_PATH.exists():
        print("[retention] No retention_config.csv found, skipping")
        return

    configs = []
    with open(RETENTION_CONFIG_PATH) as f:
        for row in csv.DictReader(f):
            if row.get("active", "1").strip() == "1":
                configs.append(row)

    if not configs:
        print("[retention] No active retention configs")
        return

    total_deleted = 0
    total_kept = 0
    summaries = []

    for cfg in configs:
        result = cleanup_source(
            data_subject=cfg["data_subject"],
            source_name=cfg["source_name"],
            source_schema=cfg["source_schema"],
            retention_days=int(cfg["retention_days"]),
        )
        total_deleted += result["deleted"]
        total_kept += result["kept"]
        summaries.append(result)

    print(f"[retention] Done — deleted {total_deleted} files, kept {total_kept} files")
    for s in summaries:
        print(f"  {s['source_name']}/{s['data_subject']}: deleted={s['deleted']}, kept={s['kept']}, errors={s['errors']}")


with DAG(
    dag_id="bronze_file_retention",
    description="Delete expired bronze parquet files based on retention_config.csv",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "retention"],
) as dag:
    retention = PythonOperator(
        task_id="run_retention",
        python_callable=run_retention,
    )
