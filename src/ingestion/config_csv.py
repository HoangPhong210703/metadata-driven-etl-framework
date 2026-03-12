"""Read layer-specific CSV config files (e.g. src2brz_config.csv)."""

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CsvTableConfig:
    id: int
    table_name: str
    table_schema_stg: str
    source_name: str
    source_schema: str
    data_subject: str
    load_strategy: str
    cursor_column: str
    initial_value: str
    primary_key: str
    load_sequence: int
    table_load_active: bool


def load_csv_config(csv_path: Path) -> list[CsvTableConfig]:
    """Read a layer config CSV and return a list of table configs."""
    configs = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            configs.append(CsvTableConfig(
                id=int(row["id"]),
                table_name=row["table_name"],
                table_schema_stg=row["table_schema_stg"],
                source_name=row["source_name"],
                source_schema=row["source_schema"],
                data_subject=row["data_subject"],
                load_strategy=row["load_strategy"],
                cursor_column=row.get("cursor_column", ""),
                initial_value=row.get("initial_value", ""),
                primary_key=row.get("primary_key", ""),
                load_sequence=int(row.get("load_sequence", 0)),
                table_load_active=row.get("table_load_active", "TRUE").upper() == "TRUE",
            ))
    return configs


def get_active_tables(configs: list[CsvTableConfig]) -> list[CsvTableConfig]:
    """Filter to active tables, sorted by load_sequence."""
    return sorted(
        [c for c in configs if c.table_load_active],
        key=lambda c: c.load_sequence,
    )


def get_data_subjects(configs: list[CsvTableConfig]) -> set[str]:
    """Return unique data_subjects from config."""
    return {c.data_subject for c in configs}


def get_active_data_subjects(configs: list[CsvTableConfig]) -> set[str]:
    """Return unique data_subjects that have at least one active table."""
    return {c.data_subject for c in configs if c.table_load_active}
