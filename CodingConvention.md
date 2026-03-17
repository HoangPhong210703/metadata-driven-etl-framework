# ETL Pipeline — Coding Conventions

## Architecture Overview

```
Button DAG → Coordinator → Get Config → Process Object → Execution
```

Each button DAG represents a single (layer, data_subject, source) combination. The coordinator parses the button's dag_id to route the flow. Two layers exist: `src2brz` (source → parquet) and `brz2stg` (parquet → postgres). After src2brz completes, it auto-triggers the corresponding brz2stg flow.

## Naming Conventions

### DAG IDs
| Type | Pattern | Example |
|------|---------|---------|
| Button | `{layer}__{data_subject}__{source}` | `src2brz__accounting__postgres_crm` |
| Get Config | `{layer}_get_config` | `src2brz_get_config` |
| Processing | `{layer}_processing` | `brz2stg_processing` |
| Execution | `src2brz_rdbms2parquet_ingestion` / `brz2stg_parquet2postgres_ingestion` | — |
| Coordinator | `coordinator` | — |

### DAG File Locations
```
dags/
├── coordinator.py
├── layer__data_subject__src/    # Button DAGs
├── layer__get_config/           # Get config DAGs
├── layer__process_object/       # Process object DAGs
└── layer__execution/            # Ingestion DAGs
```

### Schema Naming
- Staging: `stg__{data_subject}__{source_name}` (e.g. `stg__accounting__postgres_crm`)
- Silver: `silver__{domain}__{entity_type}_{name}` (e.g. `silver__crm__fact_lead`)

### dlt Pipeline Naming
- Bronze: `pipeline_name=f"bronze_{source_name}_{data_subject}"`
- Staging: `pipeline_name=f"stg_{source_name}_{data_subject}"`
- Always use `naming="direct"` (no snake_case normalization)

### Bronze Parquet Path
```
data/bronze/{data_subject}/{source_name}/{source_schema}/{table_name}/{DD}-{MM}-{YYYY}.parquet
```

## Config

### CSV Columns
`id, layer__data_subject__src, table_name, table_schema_stg, source_name, source_schema, data_subject, load_strategy, cursor_column, initial_value, primary_key, load_sequence, table_load_active`

- `table_load_active`: `1` = active, `0` = inactive
- `load_strategy`: `full`, `incremental`, or `append`
- `load_sequence`: ascending integer, controls execution order

### Config Files
- `config/src2brz_config.csv` — source-to-bronze layer
- `config/brz2stg_config.csv` — bronze-to-staging layer

### Conf Payload (passed between DAGs)
```python
{
    "button": "src2brz__accounting__postgres_crm",
    "layer": "src2brz",
    "data_subject": "accounting",
    "source": "postgres_crm",
    "tables": [...]  # added by get_config, sorted by process_object
}
```
The `button` field must be forwarded through every DAG in the chain.

## Code Patterns

### DAG Template
```python
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited

@audited
def my_task(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    # ...
    return {
        "button": conf.get("button"),
        "layer": conf.get("layer"),
        "data_subject": conf.get("data_subject"),
        "source": conf.get("source"),
        # ... task-specific fields
    }

with DAG(
    dag_id="my_dag_id",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    # ...
```

### Key Rules
- `@audited` on all task callables
- `render_template_as_native_obj=True` on DAGs that pass dicts via Jinja
- `schedule=None` on all triggered DAGs
- `wait_for_completion=False` on button DAG triggers
- Always forward `button` in return dicts
- Use `AirflowSkipException` for non-critical skips
- Print with prefix: `[component_name] message`

### Credentials
Loaded from `.dlt/secrets.toml` via `tomllib`:
```python
import tomllib
with open("/opt/airflow/.dlt/secrets.toml", "rb") as f:
    raw = tomllib.load(f)
credentials = raw["sources"][source_name]["credentials"]
```

## Audit Logging

### File Logs
- Path: `/opt/airflow/logs/audit/{dag_id}/{YYYY-MM-DD}.log`
- Format: `[YYYY-MM-DD HH:MM:SS] STATUS task_id | source=... | subject=... (duration)`

### Database
- Table: `meta.pipeline_audit` in warehouse
- Auto-created on first write
- DB failure does not break the pipeline (warning printed, execution continues)

### DagRun Notes
Set automatically by `@audited` decorator. Shows `Button: {dag_id}` in Airflow Grid view.

## Project Structure
```
.dlt/                          # dlt secrets & config
config/                        # Layer config CSVs
dags/                          # Airflow DAGs (see folder layout above)
src/ingestion/
├── config.py                  # CSV config loader (CsvTableConfig, SourceConfig, TableConfig)
├── bronze.py                  # src2brz logic (extract, normalize, load to parquet)
├── stg.py                     # brz2stg logic (parquet to postgres warehouse)
├── cli.py / stg_cli.py        # CLI entry points
└── audit/
    ├── __init__.py            # Exports: log_audit, audited
    ├── decorator.py           # @audited decorator
    ├── file_logger.py         # Write .log files
    └── db_logger.py           # Write to meta.pipeline_audit
dbt/                           # dbt project (stg + silver models)
data/bronze/                   # Parquet files
scripts/                       # Utility scripts
```

## Docker
- Executor: `LocalExecutor`
- Parallelism: `AIRFLOW__CORE__PARALLELISM` in docker-compose.yaml
- All paths inside container use `/opt/airflow/` prefix
- User will handle git commits themselves
