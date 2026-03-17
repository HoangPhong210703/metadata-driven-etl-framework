"""Clear all log files from /opt/airflow/logs/ (run inside container)."""

import shutil
from pathlib import Path

LOG_DIR = Path("/opt/airflow/logs")

if not LOG_DIR.exists():
    print(f"Log directory not found: {LOG_DIR}")
else:
    count = sum(1 for _ in LOG_DIR.rglob("*") if _.is_file())
    for item in LOG_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    print(f"Cleared {count} log files from {LOG_DIR}")
