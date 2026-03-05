from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class TableConfig:
    name: str
    load_strategy: str  # "full" or "incremental"
    data_subject: str = ""
    cursor_column: Optional[str] = None
    initial_value: Optional[str] = None
    primary_key: Optional[list[str]] = None


@dataclass
class SourceConfig:
    name: str
    schema: str
    tables: list[TableConfig]


_REQUIRED_SOURCE_FIELDS = ("name", "schema", "tables")
_REQUIRED_TABLE_FIELDS = ("name", "load_strategy", "data_subject")
_VALID_LOAD_STRATEGIES = ("full", "incremental")


def load_sources_config(config_path: Path) -> list[SourceConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    sources = []
    for raw_source in raw.get("sources") or []:
        for field in _REQUIRED_SOURCE_FIELDS:
            if field not in raw_source:
                raise ValueError(
                    f"Missing required field '{field}' in source config: {raw_source}"
                )

        tables = []
        for raw_table in raw_source.get("tables", []):
            for field in _REQUIRED_TABLE_FIELDS:
                if field not in raw_table:
                    raise ValueError(
                        f"Missing required field '{field}' in table config: {raw_table}"
                    )
            strategy = raw_table["load_strategy"]
            if strategy not in _VALID_LOAD_STRATEGIES:
                raise ValueError(
                    f"Invalid load_strategy '{strategy}' in table config: {raw_table}. "
                    f"Must be one of {_VALID_LOAD_STRATEGIES}"
                )
            if strategy == "incremental" and not raw_table.get("cursor_column"):
                raise ValueError(
                    f"Table '{raw_table['name']}' has load_strategy 'incremental' "
                    f"but no cursor_column specified"
                )

            raw_pk = raw_table.get("primary_key")
            if isinstance(raw_pk, str):
                primary_key = [raw_pk]
            elif isinstance(raw_pk, list):
                primary_key = raw_pk
            else:
                primary_key = None

            tables.append(
                TableConfig(
                    name=raw_table["name"],
                    load_strategy=strategy,
                    data_subject=raw_table["data_subject"],
                    cursor_column=raw_table.get("cursor_column"),
                    initial_value=raw_table.get("initial_value"),
                    primary_key=primary_key,
                )
            )

        sources.append(
            SourceConfig(
                name=raw_source["name"],
                schema=raw_source["schema"],
                tables=tables,
            )
        )

    return sources
