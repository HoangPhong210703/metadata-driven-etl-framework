"""File-based audit logging — writes plain text log files grouped by DAG component."""

from datetime import datetime, timezone
from pathlib import Path

AUDIT_LOG_DIR = Path("/opt/airflow/logs/audit")


def log_to_file(record: dict):
    """Append a plain text audit line to a log file under its DAG component folder.

    Structure:
        /opt/airflow/logs/audit/
        ├── coordinator/
        │   └── 2026-03-17.log
        ├── src2brz_get_config/
        │   └── 2026-03-17.log
        ├── src2brz_processing/
        │   └── 2026-03-17.log
        └── src2brz_rdbms2parquet_ingestion/
            └── 2026-03-17.log
    """
    dag_id = record.get("dag_id", "unknown")
    task_id = record.get("task_id", "unknown")
    status = record.get("status", "unknown")
    started_at = record.get("started_at")
    finished_at = record.get("finished_at")
    source = record.get("source", "")
    data_subject = record.get("data_subject", "")
    error_message = record.get("error_message", "")

    if isinstance(started_at, datetime):
        ts_str = started_at.strftime("%Y-%m-%d %H:%M:%S")
        date_str = started_at.strftime("%Y-%m-%d")
    else:
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    duration = ""
    if isinstance(started_at, datetime) and isinstance(finished_at, datetime):
        dur = (finished_at - started_at).total_seconds()
        duration = f" ({dur:.1f}s)"

    component_dir = AUDIT_LOG_DIR / dag_id
    component_dir.mkdir(parents=True, exist_ok=True)
    log_file = component_dir / f"{date_str}.log"

    line = f"[{ts_str}] {status.upper()} {task_id}"
    if source:
        line += f" | source={source}"
    if data_subject:
        line += f" | subject={data_subject}"
    line += duration

    if error_message and status == "failed":
        # Keep first line of error on same line, full traceback on next lines
        error_lines = error_message.strip().split("\n")
        line += f" | error={error_lines[0]}"
        if len(error_lines) > 1:
            line += "\n  " + "\n  ".join(error_lines[1:])

    with open(log_file, "a") as f:
        f.write(line + "\n")
