# Staging Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a staging layer that loads bronze parquet files into PostgreSQL `stg_temp` tables via dlt, then creates deduplicated "newest" tables in the `stg` schema via dbt.

**Architecture:** dlt reads parquet files from the bronze filesystem output and appends them into `stg_temp` Postgres tables (tracking processed files to avoid re-loads). dbt models then deduplicate `stg_temp` rows by primary key into `stg` schema tables. A cleanup step deletes `stg_temp` rows older than 7 days.

**Tech Stack:** Python 3.11+, dlt (filesystem source + postgres destination), dbt-postgres, PyYAML, pytest

---

### Task 1: Add `primary_key` to Config

**Files:**
- Modify: `src/ingestion/config.py`
- Modify: `tests/ingestion/test_config.py`
- Modify: `config/sources.yaml`

**Step 1: Write the failing tests**

Add these tests to `tests/ingestion/test_config.py`:

```python
def test_table_config_single_primary_key(tmp_path):
    config = tmp_path / "sources.yaml"
    config.write_text("""
sources:
  - name: db
    data_subject: x
    schema: public
    tables:
      - name: t
        load_strategy: full
        primary_key: id
""")
    sources = load_sources_config(config)
    assert sources[0].tables[0].primary_key == ["id"]


def test_table_config_composite_primary_key(tmp_path):
    config = tmp_path / "sources.yaml"
    config.write_text("""
sources:
  - name: db
    data_subject: x
    schema: public
    tables:
      - name: t
        load_strategy: full
        primary_key:
          - org_id
          - user_id
""")
    sources = load_sources_config(config)
    assert sources[0].tables[0].primary_key == ["org_id", "user_id"]


def test_table_config_no_primary_key(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[0]
    assert table.primary_key is None
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/ingestion/test_config.py -v`
Expected: FAIL — `AttributeError: 'TableConfig' object has no attribute 'primary_key'`

**Step 3: Update TableConfig and config loader**

In `src/ingestion/config.py`, add `primary_key` field to `TableConfig`:

```python
@dataclass
class TableConfig:
    name: str
    load_strategy: str  # "full" or "incremental"
    cursor_column: Optional[str] = None
    initial_value: Optional[str] = None
    primary_key: Optional[list[str]] = None
```

In the `load_sources_config` function, inside the table parsing loop, after the existing `tables.append(...)`, update the `TableConfig` construction to include:

```python
raw_pk = raw_table.get("primary_key")
if isinstance(raw_pk, str):
    primary_key = [raw_pk]
elif isinstance(raw_pk, list):
    primary_key = raw_pk
else:
    primary_key = None
```

Pass `primary_key=primary_key` to the `TableConfig` constructor.

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/ingestion/test_config.py -v`
Expected: All tests PASS (10 existing + 3 new = 13 total)

**Step 5: Update sources.yaml with primary keys**

Update `config/sources.yaml`:

```yaml
sources:
  - name: postgres_crm
    data_subject: crm
    schema: public
    tables:
      - name: project_task
        load_strategy: full
        primary_key: id

  - name: postgres_timesheet
    data_subject: hr
    schema: public
    tables:
      - name: account_account
        load_strategy: incremental
        cursor_column: write_date
        initial_value: "2024-01-01"
        primary_key: id
```

**Step 6: Commit**

```bash
git add src/ingestion/config.py tests/ingestion/test_config.py config/sources.yaml
git commit -m "feat: add primary_key support to table config (single and composite)"
```

---

### Task 2: Update Dependencies

**Files:**
- Modify: `requirements.txt`

**Step 1: Add dbt-postgres to requirements.txt**

Append to `requirements.txt`:

```
dbt-postgres>=1.7.0
```

**Step 2: Install dependencies**

Run: `pip install -r requirements.txt`

**Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add dbt-postgres dependency"
```

---

### Task 3: Scaffold dbt Project

**Files:**
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`
- Modify: `.gitignore`

**Step 1: Create dbt_project.yml**

Create `dbt/dbt_project.yml`:

```yaml
name: etl_warehouse
version: "1.0.0"

profile: warehouse

model-paths: ["models"]
test-paths: ["tests"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]
```

**Step 2: Create profiles.yml**

Create `dbt/profiles.yml`:

```yaml
warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('WAREHOUSE_HOST') }}"
      port: "{{ env_var('WAREHOUSE_PORT') | int }}"
      user: "{{ env_var('WAREHOUSE_USER') }}"
      password: "{{ env_var('WAREHOUSE_PASSWORD') }}"
      dbname: "{{ env_var('WAREHOUSE_DB') }}"
      schema: stg
      threads: 4
```

**Step 3: Update .gitignore**

Append to `.gitignore`:

```
dbt/target/
dbt/dbt_packages/
dbt/logs/
```

**Step 4: Commit**

```bash
git add dbt/dbt_project.yml dbt/profiles.yml .gitignore
git commit -m "chore: scaffold dbt project with postgres profile"
```

---

### Task 4: Stg Pipeline — Parquet → stg_temp

**Files:**
- Create: `src/ingestion/stg.py`
- Create: `tests/ingestion/test_stg.py`

**Step 1: Write the failing tests**

Create `tests/ingestion/test_stg.py`:

```python
import pytest
from src.ingestion.config import SourceConfig, TableConfig
from src.ingestion.stg import (
    get_parquet_glob,
    build_stg_pipeline,
)


@pytest.fixture
def source_config():
    return SourceConfig(
        name="postgres_crm",
        data_subject="crm",
        schema="public",
        tables=[
            TableConfig(name="project_task", load_strategy="full", primary_key=["id"]),
        ],
    )


def test_get_parquet_glob():
    glob = get_parquet_glob(
        bronze_base_url="data/bronze",
        data_subject="crm",
        source_name="postgres_crm",
        schema="public",
        table_name="project_task",
    )
    assert glob == "data/bronze/crm/postgres_crm/public/project_task"


def test_build_stg_pipeline(source_config):
    pipeline = build_stg_pipeline(
        source_config=source_config,
        warehouse_credentials="postgresql://user:pass@localhost:5432/test_db",
    )
    assert pipeline.pipeline_name == "stg_postgres_crm"
    assert pipeline.destination.destination_name == "postgres"
    assert pipeline.dataset_name == "stg_temp"
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/ingestion/test_stg.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ingestion.stg'`

**Step 3: Implement stg.py**

Create `src/ingestion/stg.py`:

```python
from pathlib import Path

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import readers

from src.ingestion.config import SourceConfig, load_sources_config


def get_parquet_glob(
    bronze_base_url: str,
    data_subject: str,
    source_name: str,
    schema: str,
    table_name: str,
) -> str:
    return f"{bronze_base_url}/{data_subject}/{source_name}/{schema}/{table_name}"


def build_stg_pipeline(
    source_config: SourceConfig,
    warehouse_credentials: str,
) -> dlt.Pipeline:
    dest = postgres(credentials=warehouse_credentials)

    return dlt.pipeline(
        pipeline_name=f"stg_{source_config.name}",
        destination=dest,
        dataset_name="stg_temp",
    )


def run_stg_ingestion(
    source_config: SourceConfig,
    bronze_base_url: str,
    warehouse_credentials: str,
) -> None:
    pipeline = build_stg_pipeline(source_config, warehouse_credentials)

    for table_config in source_config.tables:
        parquet_path = get_parquet_glob(
            bronze_base_url=bronze_base_url,
            data_subject=source_config.data_subject,
            source_name=source_config.name,
            schema=source_config.schema,
            table_name=table_config.name,
        )

        reader = readers(
            bucket_url=parquet_path,
            file_glob="*.parquet",
        ).read_parquet()

        load_info = pipeline.run(
            reader.with_name(table_config.name),
            write_disposition="append",
        )
        print(f"[stg_{source_config.name}] Loaded {table_config.name}: {load_info}")


def run_all_stg_sources(
    config_path: Path,
    bronze_base_url: str,
    warehouse_credentials: str,
) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        print(f"[stg_{source_config.name}] Starting stg ingestion...")
        run_stg_ingestion(source_config, bronze_base_url, warehouse_credentials)
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/ingestion/test_stg.py -v`
Expected: All 2 tests PASS

**Step 5: Commit**

```bash
git add src/ingestion/stg.py tests/ingestion/test_stg.py
git commit -m "feat: add stg pipeline to load bronze parquet into postgres stg_temp"
```

---

### Task 5: dbt Staging Models

**Files:**
- Create: `dbt/models/stg/_stg_sources.yml`
- Create: `dbt/models/stg/stg_project_task.sql`
- Create: `dbt/models/stg/stg_account_account.sql`

**Step 1: Create dbt source definition**

Create `dbt/models/stg/_stg_sources.yml`:

```yaml
version: 2

sources:
  - name: stg_temp
    schema: stg_temp
    tables:
      - name: project_task
      - name: account_account
```

**Step 2: Create stg_project_task model (full load → table materialization)**

`project_task` uses `load_strategy: full`, so every parquet file contains the complete table. A simple `DISTINCT ON` with full refresh is correct — the latest load always has all rows.

Create `dbt/models/stg/stg_project_task.sql`:

```sql
{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg_temp', 'project_task') }}
order by id, _dlt_load_id desc
```

**Step 3: Create stg_account_account model (incremental load → incremental materialization)**

`account_account` uses `load_strategy: incremental`, so subsequent parquet files only contain changed rows. The dbt model must use `materialized='incremental'` with merge so that unchanged rows persist in the stg table even after stg_temp cleanup purges old loads.

Create `dbt/models/stg/stg_account_account.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    schema='stg',
    on_schema_change='append_new_columns'
) }}

select distinct on (id)
    *
from {{ source('stg_temp', 'account_account') }}
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
{% endif %}
order by id, _dlt_load_id desc
```

**Important pattern note:** Tables with `load_strategy: full` use `materialized='table'`. Tables with `load_strategy: incremental` use `materialized='incremental'` with `unique_key` set to their primary key. This ensures incremental tables don't lose rows when stg_temp is cleaned up.

**Step 4: Verify dbt can parse the project (no DB connection needed)**

Run: `cd dbt && dbt parse --profiles-dir .`
Expected: May warn about missing env vars but should parse models successfully. If env vars are required, set dummy values first:
```bash
export WAREHOUSE_HOST=localhost WAREHOUSE_PORT=5432 WAREHOUSE_USER=x WAREHOUSE_PASSWORD=x WAREHOUSE_DB=x
cd dbt && dbt parse --profiles-dir .
```

**Step 5: Commit**

```bash
git add dbt/models/stg/
git commit -m "feat: add dbt stg models for project_task and account_account"
```

---

### Task 6: Stg Cleanup Function (7-Day Retention)

**Files:**
- Modify: `src/ingestion/stg.py`
- Modify: `tests/ingestion/test_stg.py`

**Step 1: Write the failing test**

Add to `tests/ingestion/test_stg.py`:

```python
from src.ingestion.stg import build_cleanup_query


def test_build_cleanup_query_single_table():
    query = build_cleanup_query("project_task", retention_days=7)
    assert "stg_temp.project_task" in query
    assert "7 days" in query


def test_build_cleanup_query_custom_retention():
    query = build_cleanup_query("account_account", retention_days=30)
    assert "stg_temp.account_account" in query
    assert "30 days" in query
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/ingestion/test_stg.py::test_build_cleanup_query_single_table -v`
Expected: FAIL — `ImportError: cannot import name 'build_cleanup_query'`

**Step 3: Implement build_cleanup_query**

Add to `src/ingestion/stg.py`:

```python
import sqlalchemy


def build_cleanup_query(table_name: str, retention_days: int = 7) -> str:
    return (
        f"DELETE FROM stg_temp.{table_name} "
        f"WHERE _dlt_load_id IN ("
        f"SELECT load_id FROM stg_temp._dlt_loads "
        f"WHERE inserted_at < NOW() - INTERVAL '{retention_days} days'"
        f")"
    )


def run_stg_cleanup(
    source_config: SourceConfig,
    warehouse_credentials: str,
    retention_days: int = 7,
) -> None:
    engine = sqlalchemy.create_engine(warehouse_credentials)

    for table_config in source_config.tables:
        query = build_cleanup_query(table_config.name, retention_days)
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query))
            conn.commit()
            print(
                f"[stg_{source_config.name}] Cleanup {table_config.name}: "
                f"deleted {result.rowcount} rows older than {retention_days} days"
            )
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/ingestion/test_stg.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/ingestion/stg.py tests/ingestion/test_stg.py
git commit -m "feat: add stg_temp cleanup with configurable retention"
```

---

### Task 7: Update Secrets Example

**Files:**
- Modify: `.dlt/secrets.toml.example`

**Step 1: Add warehouse credentials section**

Append to `.dlt/secrets.toml.example`:

```toml

[destinations.warehouse]
credentials = "postgresql://user:password@localhost:5432/warehouse_db"
```

**Step 2: Commit**

```bash
git add .dlt/secrets.toml.example
git commit -m "chore: add warehouse credentials to secrets example"
```

---

### Task 8: Stg CLI Entry Point

**Files:**
- Create: `src/ingestion/stg_cli.py`

**Step 1: Implement the CLI**

Create `src/ingestion/stg_cli.py`:

```python
import argparse
import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from src.ingestion.config import load_sources_config
from src.ingestion.stg import run_stg_ingestion, run_stg_cleanup


def load_warehouse_credentials(secrets_path: Path) -> str:
    import tomllib

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {secrets_path}. "
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in credentials."
        )

    with open(secrets_path, "rb") as f:
        raw = tomllib.load(f)

    warehouse = raw.get("destinations", {}).get("warehouse", {})
    credentials = warehouse.get("credentials")
    if not credentials:
        raise ValueError("No warehouse credentials found in secrets at [destinations.warehouse]")

    return credentials


def set_dbt_env_vars(credentials: str) -> None:
    """Parse connection string and set env vars for dbt profiles.yml."""
    parsed = urlparse(credentials)
    os.environ["WAREHOUSE_HOST"] = parsed.hostname or "localhost"
    os.environ["WAREHOUSE_PORT"] = str(parsed.port or 5432)
    os.environ["WAREHOUSE_USER"] = parsed.username or ""
    os.environ["WAREHOUSE_PASSWORD"] = parsed.password or ""
    os.environ["WAREHOUSE_DB"] = (parsed.path or "").lstrip("/")


def run_dbt(dbt_project_dir: Path) -> None:
    """Run dbt models for the stg layer."""
    profiles_dir = dbt_project_dir
    result = subprocess.run(
        ["dbt", "run", "--select", "stg", "--profiles-dir", str(profiles_dir)],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")


def main():
    parser = argparse.ArgumentParser(description="Staging layer ingestion")
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
        help="Path to secrets.toml with credentials",
    )
    parser.add_argument(
        "--bronze-url",
        type=str,
        default="data/bronze",
        help="Base directory of bronze parquet files",
    )
    parser.add_argument(
        "--dbt-dir",
        type=Path,
        default=Path("dbt"),
        help="Path to dbt project directory",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Number of days to retain in stg_temp (default: 7)",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt run (only load parquet into stg_temp)",
    )
    args = parser.parse_args()

    sources = load_sources_config(args.config)
    warehouse_credentials = load_warehouse_credentials(args.secrets)

    # Step 1: dlt — load parquet → stg_temp
    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue
        print(f"[stg_{source_config.name}] Loading parquet into stg_temp...")
        run_stg_ingestion(source_config, args.bronze_url, warehouse_credentials)

    # Step 2: dbt — build stg newest tables
    if not args.skip_dbt:
        print("[stg] Running dbt models...")
        set_dbt_env_vars(warehouse_credentials)
        run_dbt(args.dbt_dir)

    # Step 3: cleanup — delete old stg_temp rows
    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue
        run_stg_cleanup(source_config, warehouse_credentials, args.retention_days)


if __name__ == "__main__":
    main()
```

**Step 2: Verify it runs**

Run: `python -m src.ingestion.stg_cli --help`
Expected: Shows help text with `--config`, `--secrets`, `--bronze-url`, `--dbt-dir`, `--source`, `--retention-days`, `--skip-dbt` options

**Step 3: Commit**

```bash
git add src/ingestion/stg_cli.py
git commit -m "feat: add CLI entry point for stg layer ingestion"
```

---

### Task 9: Airflow DAG for Stg Ingestion

**Files:**
- Create: `dags/stg_ingestion.py`

**Step 1: Implement the DAG**

Create `dags/stg_ingestion.py`:

```python
"""Stg ingestion DAG — loads bronze parquet into postgres, runs dbt, cleans up."""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_sources_config
from src.ingestion.stg import run_stg_ingestion, run_stg_cleanup

CONFIG_PATH = Path("/opt/airflow/config/sources.yaml")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BASE_URL = "/opt/airflow/data/bronze"
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
RETENTION_DAYS = 7


def _load_warehouse_credentials() -> str:
    import tomllib

    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def _set_dbt_env_vars(credentials: str) -> None:
    from urllib.parse import urlparse

    parsed = urlparse(credentials)
    os.environ["WAREHOUSE_HOST"] = parsed.hostname or "localhost"
    os.environ["WAREHOUSE_PORT"] = str(parsed.port or 5432)
    os.environ["WAREHOUSE_USER"] = parsed.username or ""
    os.environ["WAREHOUSE_PASSWORD"] = parsed.password or ""
    os.environ["WAREHOUSE_DB"] = (parsed.path or "").lstrip("/")


def load_parquet_to_stg_temp(source_name: str, **kwargs):
    sources = load_sources_config(CONFIG_PATH)
    credentials = _load_warehouse_credentials()
    source_config = next(s for s in sources if s.name == source_name)
    run_stg_ingestion(source_config, BRONZE_BASE_URL, credentials)


def run_dbt_stg(**kwargs):
    credentials = _load_warehouse_credentials()
    _set_dbt_env_vars(credentials)
    result = subprocess.run(
        ["dbt", "run", "--select", "stg", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")


def cleanup_stg_temp(source_name: str, **kwargs):
    sources = load_sources_config(CONFIG_PATH)
    credentials = _load_warehouse_credentials()
    source_config = next(s for s in sources if s.name == source_name)
    run_stg_cleanup(source_config, credentials, RETENTION_DAYS)


with DAG(
    dag_id="stg_ingestion",
    description="Load bronze parquet into stg_temp, run dbt stg models, cleanup",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stg", "ingestion", "dbt"],
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_ingestion",
        external_task_id=None,
        mode="poke",
        timeout=3600,
        poke_interval=60,
    )

    sources = load_sources_config(CONFIG_PATH) if CONFIG_PATH.exists() else []

    load_tasks = []
    for source in sources:
        task = PythonOperator(
            task_id=f"load_{source.name}_to_stg_temp",
            python_callable=load_parquet_to_stg_temp,
            op_kwargs={"source_name": source.name},
        )
        wait_for_bronze >> task
        load_tasks.append(task)

    dbt_task = PythonOperator(
        task_id="dbt_run_stg",
        python_callable=run_dbt_stg,
    )

    for load_task in load_tasks:
        load_task >> dbt_task

    for source in sources:
        cleanup_task = PythonOperator(
            task_id=f"cleanup_{source.name}_stg_temp",
            python_callable=cleanup_stg_temp,
            op_kwargs={"source_name": source.name},
        )
        dbt_task >> cleanup_task
```

**Step 2: Commit**

```bash
git add dags/stg_ingestion.py
git commit -m "feat: add Airflow DAG for stg layer ingestion"
```

---

### Task 10: Update Docker Compose for dbt

**Files:**
- Modify: `docker-compose.yaml`
- Modify: `Dockerfile` (if dbt-postgres not already installed)

**Step 1: Add dbt volume mount to docker-compose.yaml**

In the `x-airflow-common` volumes section, add:

```yaml
    - ./dbt:/opt/airflow/dbt
```

**Step 2: Verify Dockerfile installs requirements.txt (which now includes dbt-postgres)**

Check that `Dockerfile` runs `pip install -r requirements.txt`. If so, dbt-postgres will be installed automatically. If not, add:

```dockerfile
RUN pip install dbt-postgres>=1.7.0
```

**Step 3: Commit**

```bash
git add docker-compose.yaml Dockerfile
git commit -m "chore: add dbt volume mount and ensure dbt-postgres installed in Docker"
```

---

### Task 11: Run All Tests and Final Verification

**Step 1: Run full test suite**

Run: `python -m pytest tests/ -v`
Expected: All tests PASS (13 config + 3 bronze + 4 stg = 20 tests)

**Step 2: Verify project structure**

Run: `find . -not -path './.git/*' -not -path './.venv/*' -not -path './__pycache__/*' -not -path './data/*' -not -path './logs/*' -not -path './.dlt/pipelines/*' | sort`

Expected new/modified files:

```
./config/sources.yaml              (modified: added primary_key)
./dags/stg_ingestion.py            (new)
./dbt/dbt_project.yml              (new)
./dbt/profiles.yml                 (new)
./dbt/models/stg/_stg_sources.yml  (new)
./dbt/models/stg/stg_project_task.sql       (new)
./dbt/models/stg/stg_account_account.sql    (new)
./src/ingestion/stg.py             (new)
./src/ingestion/stg_cli.py         (new)
./tests/ingestion/test_stg.py      (new)
```

**Step 3: Final commit (if any remaining changes)**

```bash
git status
# If clean, nothing to do.
```
