"""Pipeline audit package — exports log_audit and audited decorator."""

from datetime import datetime

from src.ingestion.audit.file_logger import log_to_file
from src.ingestion.audit.db_logger import log_to_db


def log_audit(
    run_id: str,
    dag_id: str,
    task_id: str,
    status: str,
    layer: str = None,
    source: str = None,
    data_subject: str = None,
    table_name: str = None,
    row_count: int = None,
    error_message: str = None,
    started_at: datetime = None,
    finished_at: datetime = None,
):
    """Write audit record to both file and database."""
    record = {
        "run_id": run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "layer": layer,
        "source": source,
        "data_subject": data_subject,
        "table_name": table_name,
        "status": status,
        "row_count": row_count,
        "error_message": error_message,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    # Always write to file (lightweight, no external dependency)
    log_to_file(record)

    # Write to DB — don't let DB failure break the pipeline
    try:
        log_to_db(record)
    except Exception as e:
        print(f"[audit] WARNING: Failed to write audit to DB: {e}")


from src.ingestion.audit.decorator import audited  # noqa: E402

__all__ = ["log_audit", "audited"]
