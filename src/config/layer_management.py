"""Layer management — reads layer_management_config.csv to determine
whether to auto-trigger the next layer after the current one completes."""

import csv
from pathlib import Path

LAYER_MGMT_PATH = Path("/opt/airflow/config/layer_management_config.csv")


def get_next_layer(current_layer: str, data_subject: str, source: str) -> str | None:
    """Return the target layer's button dag_id if auto_trigger is enabled, else None."""
    if not LAYER_MGMT_PATH.exists():
        return None

    with open(LAYER_MGMT_PATH) as f:
        for row in csv.DictReader(f):
            if (
                row["source_layer"] == current_layer
                and row["data_subject"] == data_subject
                and row["source"] == source
                and row.get("active", "1").strip() in ("1", "TRUE", "true")
                and row.get("auto_trigger", "0").strip() in ("1", "TRUE", "true")
            ):
                target_layer = row["target_layer"]
                return f"{target_layer}__{data_subject}__{source}"

    return None
