"""Bronze file retention — deletes parquet files older than the configured retention window."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

BRONZE_DIR = Path("/opt/airflow/data/bronze")
DATE_FORMAT = "%d-%m-%Y"


def cleanup_source(
    data_subject: str,
    source_name: str,
    source_schema: str,
    retention_days: int,
) -> dict:
    """Delete parquet files older than retention_days for a single source.

    Returns a summary dict with counts of deleted and kept files.
    """
    source_dir = BRONZE_DIR / data_subject / source_name / source_schema
    if not source_dir.exists():
        print(f"[retention] Directory not found: {source_dir}")
        return {"source_name": source_name, "data_subject": data_subject, "deleted": 0, "kept": 0, "errors": 0}

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=retention_days)
    deleted = 0
    kept = 0
    errors = 0

    for table_dir in source_dir.iterdir():
        if not table_dir.is_dir() or table_dir.name.startswith("_dlt"):
            continue

        for parquet_file in table_dir.glob("*.parquet"):
            try:
                file_date = datetime.strptime(parquet_file.stem, DATE_FORMAT).date()
                if file_date < cutoff:
                    parquet_file.unlink()
                    deleted += 1
                    print(f"[retention] Deleted: {parquet_file.relative_to(BRONZE_DIR)}")
                else:
                    kept += 1
            except ValueError:
                errors += 1
                print(f"[retention] Skipping (unexpected name): {parquet_file.name}")

    return {
        "source_name": source_name,
        "data_subject": data_subject,
        "deleted": deleted,
        "kept": kept,
        "errors": errors,
    }
