# Bronze Ingestion Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a config-driven dlt ingestion pipeline that extracts tables from PostgreSQL and MariaDB sources into date-partitioned parquet files on the local filesystem.

**Architecture:** One dlt pipeline per source database, configured via `config/sources.yaml`. Each pipeline uses dlt's `sql_database` source with `pyarrow` backend and `filesystem` destination. Tables support full or incremental extraction strategies. Output path: `data/bronze/{data_subject}/{src_db}/{src_schema}/{table_name}/{DD-MM-YYYY}.parquet`.

**Tech Stack:** Python 3.11+, dlt (with `sql_database`, `filesystem`, `parquet` extras), PyYAML, pytest

---

### Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `.gitignore`
- Create: `src/__init__.py`
- Create: `src/ingestion/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/ingestion/__init__.py`
- Create: `.dlt/config.toml`
- Create: `.dlt/secrets.toml.example`

**Step 1: Create requirements.txt**

```
dlt[sql_database,filesystem,parquet]>=1.0.0
psycopg2-binary
pymysql
pyyaml
pytest
```

**Step 2: Create .gitignore**

```
__pycache__/
*.pyc
.venv/
venv/
data/
.dlt/secrets.toml
*.egg-info/
.pytest_cache/
```

**Step 3: Create empty __init__.py files**

Create empty files:
- `src/__init__.py`
- `src/ingestion/__init__.py`
- `tests/__init__.py`
- `tests/ingestion/__init__.py`

**Step 4: Create .dlt/config.toml**

```toml
[normalize.data_writer]
disable_compression = false
```

**Step 5: Create .dlt/secrets.toml.example**

```toml
# Copy this file to secrets.toml and fill in your credentials.
# secrets.toml is gitignored and should never be committed.

[sources.postgres_crm]
credentials = "postgresql://user:password@localhost:5432/crm_db"

[sources.postgres_timesheet]
credentials = "postgresql://user:password@localhost:5432/timesheet_db"

[sources.mariadb_erp]
credentials = "mysql+pymysql://user:password@localhost:3306/erp_db"
```

**Step 6: Create virtual environment and install dependencies**

Run:
```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```

**Step 7: Commit**

```bash
git add requirements.txt .gitignore src/ tests/ .dlt/config.toml .dlt/secrets.toml.example
git commit -m "chore: scaffold project structure and dependencies"
```

---

### Task 2: YAML Config Loader

**Files:**
- Create: `config/sources.yaml`
- Create: `src/ingestion/config.py`
- Create: `tests/ingestion/test_config.py`

**Step 1: Create the example sources.yaml**

Create `config/sources.yaml`:

```yaml
sources:
  - name: postgres_crm
    data_subject: crm
    schema: public
    tables:
      - name: customers
        load_strategy: full
      - name: orders
        load_strategy: incremental
        cursor_column: updated_at
        initial_value: "2024-01-01"

  - name: postgres_timesheet
    data_subject: hr
    schema: public
    tables:
      - name: timesheets
        load_strategy: incremental
        cursor_column: updated_at
        initial_value: "2024-01-01"

  - name: mariadb_erp
    data_subject: erp
    schema: erp_db
    tables:
      - name: invoices
        load_strategy: incremental
        cursor_column: modified_at
        initial_value: "2024-01-01"
```

**Step 2: Write the failing tests**

Create `tests/ingestion/test_config.py`:

```python
import pytest
from pathlib import Path
from src.ingestion.config import load_sources_config, SourceConfig, TableConfig


@pytest.fixture
def sample_config_path(tmp_path):
    config_content = """
sources:
  - name: test_db
    data_subject: test
    schema: public
    tables:
      - name: users
        load_strategy: full
      - name: events
        load_strategy: incremental
        cursor_column: created_at
        initial_value: "2024-01-01"
"""
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(config_content)
    return config_file


def test_load_sources_config_returns_list(sample_config_path):
    sources = load_sources_config(sample_config_path)
    assert isinstance(sources, list)
    assert len(sources) == 1


def test_source_config_fields(sample_config_path):
    sources = load_sources_config(sample_config_path)
    source = sources[0]
    assert isinstance(source, SourceConfig)
    assert source.name == "test_db"
    assert source.data_subject == "test"
    assert source.schema == "public"
    assert len(source.tables) == 2


def test_table_config_full_strategy(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[0]
    assert isinstance(table, TableConfig)
    assert table.name == "users"
    assert table.load_strategy == "full"
    assert table.cursor_column is None
    assert table.initial_value is None


def test_table_config_incremental_strategy(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[1]
    assert table.name == "events"
    assert table.load_strategy == "incremental"
    assert table.cursor_column == "created_at"
    assert table.initial_value == "2024-01-01"


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_sources_config(Path("/nonexistent/sources.yaml"))


def test_load_config_missing_required_field(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("sources:\n  - name: test_db\n    tables: []")
    with pytest.raises(ValueError, match="data_subject"):
        load_sources_config(bad_config)
```

**Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/ingestion/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ingestion.config'`

**Step 4: Implement the config loader**

Create `src/ingestion/config.py`:

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class TableConfig:
    name: str
    load_strategy: str  # "full" or "incremental"
    cursor_column: Optional[str] = None
    initial_value: Optional[str] = None


@dataclass
class SourceConfig:
    name: str
    data_subject: str
    schema: str
    tables: list[TableConfig]


_REQUIRED_SOURCE_FIELDS = ("name", "data_subject", "schema", "tables")
_REQUIRED_TABLE_FIELDS = ("name", "load_strategy")


def load_sources_config(config_path: Path) -> list[SourceConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    sources = []
    for raw_source in raw.get("sources", []):
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
            tables.append(
                TableConfig(
                    name=raw_table["name"],
                    load_strategy=raw_table["load_strategy"],
                    cursor_column=raw_table.get("cursor_column"),
                    initial_value=raw_table.get("initial_value"),
                )
            )

        sources.append(
            SourceConfig(
                name=raw_source["name"],
                data_subject=raw_source["data_subject"],
                schema=raw_source["schema"],
                tables=tables,
            )
        )

    return sources
```

**Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/ingestion/test_config.py -v`
Expected: All 7 tests PASS

**Step 6: Commit**

```bash
git add config/sources.yaml src/ingestion/config.py tests/ingestion/test_config.py
git commit -m "feat: add YAML config loader for source definitions"
```

---

### Task 3: Bronze Pipeline Builder

**Files:**
- Create: `src/ingestion/bronze.py`
- Create: `tests/ingestion/test_bronze.py`

**Step 1: Write the failing tests**

Create `tests/ingestion/test_bronze.py`:

```python
import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.config import SourceConfig, TableConfig
from src.ingestion.bronze import build_pipeline, build_layout, run_source_ingestion


@pytest.fixture
def source_config():
    return SourceConfig(
        name="test_pg",
        data_subject="crm",
        schema="public",
        tables=[
            TableConfig(name="customers", load_strategy="full"),
            TableConfig(
                name="orders",
                load_strategy="incremental",
                cursor_column="updated_at",
                initial_value="2024-01-01",
            ),
        ],
    )


def test_build_layout():
    layout = build_layout()
    assert "{data_subject}" in layout
    assert "{src_db}" in layout
    assert "{src_schema}" in layout
    assert "{table_name}" in layout
    assert "{DD}" in layout
    assert "{MM}" in layout
    assert "{YYYY}" in layout
    assert layout.endswith(".{ext}")


def test_build_pipeline_returns_dlt_pipeline(source_config):
    pipeline = build_pipeline(source_config, bucket_url="/tmp/test_bronze")
    assert pipeline.pipeline_name == "bronze_test_pg"
    assert pipeline.destination.destination_name == "filesystem"


def test_build_pipeline_extra_placeholders(source_config):
    pipeline = build_pipeline(source_config, bucket_url="/tmp/test_bronze")
    # Verify the pipeline was created (destination config is internal to dlt,
    # so we just confirm it doesn't raise)
    assert pipeline is not None
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/ingestion/test_bronze.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ingestion.bronze'`

**Step 3: Implement the pipeline builder**

Create `src/ingestion/bronze.py`:

```python
import datetime
from pathlib import Path

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database

from src.ingestion.config import SourceConfig, TableConfig, load_sources_config


def build_layout() -> str:
    return "{data_subject}/{src_db}/{src_schema}/{table_name}/{DD}-{MM}-{YYYY}.{ext}"


def build_pipeline(source_config: SourceConfig, bucket_url: str) -> dlt.Pipeline:
    dest = filesystem(
        bucket_url=bucket_url,
        layout=build_layout(),
        extra_placeholders={
            "data_subject": source_config.data_subject,
            "src_db": source_config.name,
            "src_schema": source_config.schema,
        },
    )

    return dlt.pipeline(
        pipeline_name=f"bronze_{source_config.name}",
        destination=dest,
        dataset_name="bronze",
    )


def run_source_ingestion(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
) -> None:
    pipeline = build_pipeline(source_config, bucket_url)

    table_names = [t.name for t in source_config.tables]
    source = sql_database(
        credentials=credentials,
        schema=source_config.schema,
        table_names=table_names,
        backend="pyarrow",
    )

    for table_config in source_config.tables:
        if table_config.load_strategy == "incremental" and table_config.cursor_column:
            resource = source.resources[table_config.name]
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    table_config.cursor_column,
                    initial_value=table_config.initial_value,
                ),
            )

    load_info = pipeline.run(source, write_disposition="append", loader_file_format="parquet")
    print(f"[{source_config.name}] Load complete: {load_info}")


def run_all_sources(config_path: Path, bucket_url: str, secrets: dict[str, str]) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials found")
            continue

        run_source_ingestion(source_config, bucket_url, credentials)
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/ingestion/test_bronze.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add src/ingestion/bronze.py tests/ingestion/test_bronze.py
git commit -m "feat: add bronze pipeline builder with configurable layout and incremental support"
```

---

### Task 4: CLI Entry Point

**Files:**
- Create: `src/ingestion/cli.py`

**Step 1: Implement the CLI entry point**

Create `src/ingestion/cli.py`:

```python
import argparse
from pathlib import Path

import dlt.common.configuration.specs.config_providers as providers

from src.ingestion.config import load_sources_config
from src.ingestion.bronze import run_source_ingestion


def load_secrets(secrets_path: Path) -> dict[str, str]:
    """Read credentials from .dlt/secrets.toml keyed by source name."""
    import tomllib

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {secrets_path}. "
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in credentials."
        )

    with open(secrets_path, "rb") as f:
        raw = tomllib.load(f)

    secrets = {}
    for key, value in raw.get("sources", {}).items():
        if isinstance(value, dict) and "credentials" in value:
            secrets[key] = value["credentials"]

    return secrets


def main():
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/sources.yaml"),
        help="Path to sources.yaml config file",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".dlt/secrets.toml"),
        help="Path to secrets.toml with database credentials",
    )
    parser.add_argument(
        "--bucket-url",
        type=str,
        default="data/bronze",
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    args = parser.parse_args()

    sources = load_sources_config(args.config)
    secrets = load_secrets(args.secrets)

    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue

        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials in secrets file")
            continue

        print(f"[{source_config.name}] Starting ingestion...")
        run_source_ingestion(source_config, args.bucket_url, credentials)


if __name__ == "__main__":
    main()
```

**Step 2: Verify it runs (dry check — no real DBs needed)**

Run: `python -m src.ingestion.cli --help`
Expected: Shows help text with `--config`, `--secrets`, `--bucket-url`, `--source` options

**Step 3: Commit**

```bash
git add src/ingestion/cli.py
git commit -m "feat: add CLI entry point for bronze ingestion"
```

---

### Task 5: Run All Tests and Final Verification

**Step 1: Run full test suite**

Run: `python -m pytest tests/ -v`
Expected: All tests PASS

**Step 2: Verify project structure**

Run: `find . -not -path './.git/*' -not -path './.venv/*' -not -path './__pycache__/*' | sort`

Expected structure:
```
.
./config/sources.yaml
./docs/plans/2026-03-03-bronze-ingestion-design.md
./docs/plans/2026-03-03-bronze-ingestion-plan.md
./.dlt/config.toml
./.dlt/secrets.toml.example
./.gitignore
./requirements.txt
./src/__init__.py
./src/ingestion/__init__.py
./src/ingestion/bronze.py
./src/ingestion/cli.py
./src/ingestion/config.py
./tests/__init__.py
./tests/ingestion/__init__.py
./tests/ingestion/test_bronze.py
./tests/ingestion/test_config.py
```

**Step 3: Final commit (if any remaining changes)**

```bash
git status
# If clean, nothing to do. If changes exist, commit them.
```
