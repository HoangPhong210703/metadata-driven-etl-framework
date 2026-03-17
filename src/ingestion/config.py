"""Unified config module — reads CSV config files, provides SourceConfig/TableConfig."""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class TableConfig:
    name: str
    load_strategy: str  # "full", "incremental", or "append"
    data_subject: str = ""
    layer_subject_source: str = ""
    cursor_column: Optional[str] = None
    initial_value: Optional[str] = None
    primary_key: Optional[list[str]] = None


@dataclass
class SourceConfig:
    name: str
    schema: str
    tables: list[TableConfig]


@dataclass
class CsvTableConfig:
    id: int
    layer_subject_source: str
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
    if not csv_path.exists():
        raise FileNotFoundError(f"Config file not found: {csv_path}")

    configs = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            configs.append(CsvTableConfig(
                id=int(row["id"]),
                layer_subject_source=row["layer_subject_source"],
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
                table_load_active=row.get("table_load_active", "1").upper() in ("TRUE", "1"),
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


def csv_to_source_configs(configs: list[CsvTableConfig]) -> list[SourceConfig]:
    """Group CSV table configs into SourceConfig objects."""
    sources: dict[str, dict] = {}
    for c in configs:
        key = c.source_name
        if key not in sources:
            sources[key] = {"name": c.source_name, "schema": c.source_schema, "tables": []}
        sources[key]["tables"].append(TableConfig(
            name=c.table_name,
            load_strategy=c.load_strategy,
            data_subject=c.data_subject,
            layer_subject_source=c.layer_subject_source,
            cursor_column=c.cursor_column or None,
            initial_value=c.initial_value or None,
            primary_key=[c.primary_key] if c.primary_key else None,
        ))
    return [SourceConfig(**v) for v in sources.values()]


def load_source_configs(csv_path: Path) -> list[SourceConfig]:
    """Load CSV config and return as SourceConfig objects (active tables only)."""
    configs = load_csv_config(csv_path)
    active = get_active_tables(configs)
    return csv_to_source_configs(active)
