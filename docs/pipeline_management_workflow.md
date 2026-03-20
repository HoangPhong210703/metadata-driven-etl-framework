# Pipeline Management Workflow

This document explains how to manage the ETL pipeline system after deployment and how to extend it with new features.

---

## 1. System Overview

### Architecture

```
                         AIRFLOW UI
                             |
                      [Button DAGs]
                     /       |       \
    src2brz__accounting  src2brz__accounting  brz2sil__accounting
    __postgres_crm       __postgres_timesheet __postgres_crm
                     \       |       /
                       coordinator
                      /            \
               src2brz_get_config   brz2sil_get_config
                     |                     |
            src2brz__process_object  brz2sil__process_object
                     |                     |
     src2brz_rdbms2parquet_ingestion  brz2stg_parquet2postgres_ingestion
          |                                |
    [trigger_next_layer]            [Dataset event]
          |                                |
       coordinator                  stg2sil__process_whdata
          |                         (scheduled on Dataset)
    brz2sil_get_config                     |
          |                    dbt stg → snapshot → silver → test
         ...
```

### Data Flow

```
Source DB ──[dlt]──> Parquet files ──[dlt]──> Staging schemas ──[dbt]──> Silver schemas
(RDBMS)          /data/bronze/         stg__{subject}__{source}     sil__{domain}
```

### Layer Summary

| Layer | Purpose | Trigger | Output |
|-------|---------|---------|--------|
| **src2brz** | Extract from RDBMS to parquet | Button click | `/data/bronze/{subject}/{source}/{schema}/{table}/{date}.parquet` |
| **brz2stg** | Load parquet into warehouse | Auto-triggered after src2brz | `stg__{data_subject}__{source_name}` postgres schema |
| **stg2sil** | dbt transforms (clean, deduplicate, model) | Dataset event (all stg schemas updated) | `sil__{domain}` postgres schema |

### DAG Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| Button DAG | `{layer}__{subject}__{source}` | `src2brz__accounting__postgres_crm` |
| Coordinator | `coordinator` | `coordinator` |
| Get Config | `{layer}_get_config` (single `_`) | `src2brz_get_config` |
| Process Object | `{layer}__process_object` (double `__`) | `src2brz__process_object` |
| Execution | descriptive name | `src2brz_rdbms2parquet_ingestion` |

---

## 2. Day-to-Day Operations

### Running the Pipeline

**Manual trigger (full flow):**
1. Go to Airflow UI
2. Find the button DAG (e.g. `src2brz__accounting__postgres_crm`)
3. Click "Trigger DAG" (no conf needed)
4. The pipeline flows automatically:
   - button → coordinator → get_config → process_object → execution
   - If auto-trigger is enabled, src2brz completion triggers brz2sil automatically

**Running a single layer:**
- To run only src2brz: trigger `src2brz__accounting__postgres_crm`
- To run only brz2sil: trigger `brz2sil__accounting__postgres_crm`
- stg2sil triggers automatically via Dataset events when all required stg schemas are updated

### Monitoring

**Airflow Grid View:**
- Each DAG run has a **note** showing which button triggered it (set by the `@audited` decorator)
- Task status: green = success, red = failed, yellow = running

**Audit Logs:**
- File logs: `/opt/airflow/logs/audit/{dag_id}/{YYYY-MM-DD}.log`
- Database logs: `meta.pipeline_audit` table in warehouse

**Key things to check:**
- `src2brz_rdbms2parquet_ingestion` → check `fetch_tables` task log for row counts
- `brz2stg_parquet2postgres_ingestion` → check `load_to_warehouse` task log for load summary
- `stg2sil__process_whdata` → check `run_dbt_stg` / `run_dbt_silver` for dbt output

### Troubleshooting

**Pipeline stuck on "Subsequent run" (no data extracted):**
```bash
# Inside the Airflow container:
# Clear local dlt pipeline state
rm -rf ~/.dlt/pipelines/bronze_*
# Clear destination state
rm -rf /opt/airflow/data/bronze/{subject}/{source}/{schema}/{table}/_dlt_pipeline_state
```

**dbt model fails with "relation does not exist":**
- Check that brz2stg has loaded the table into the warehouse
- Verify the schema name in `_silver_sources.yml` matches what dlt creates
- dlt uses `dataset_name=stg__{subject}__{source}` which postgres stores as-is

**stg2sil doesn't trigger automatically:**
- stg2sil is scheduled on Dataset events — it only triggers when ALL datasets in its schedule list have been updated
- Check that `brz2stg_parquet2postgres_ingestion` emits the Dataset event (check the `load_to_warehouse` task log for "Emitted dataset event")
- In Airflow UI: go to Datasets tab to see which datasets have been updated

---

## 3. Configuration Reference

All pipeline behavior is driven by 4 CSV files in `/config/`.

### `src2brz_config.csv` — Source extraction config

Defines which tables to extract from which source databases.

| Column | Description | Example |
|--------|-------------|---------|
| `id` | Unique row ID | `1` |
| `table_name` | Table name in source database | `account_account` |
| `source_name` | Source connection key (matches `.dlt/secrets.toml`) | `postgres_crm` |
| `source_schema` | Schema in source database | `public` |
| `data_subject` | Business domain grouping | `accounting` |
| `load_strategy` | `full`, `incremental`, or `append` | `incremental` |
| `cursor_column` | Column for incremental detection | `write_date` |
| `initial_value` | Starting cursor value (first run) | `1/1/2024` |
| `primary_key` | Natural key column | `id` |
| `load_sequence` | Execution order (lower = first) | `10` |
| `table_load_active` | `1` = extract, `0` = skip | `1` |

### `brz2sil_config.csv` — Bronze-to-silver config

Same structure as `src2brz_config.csv`. Defines which tables to load from parquet into the warehouse.

### `dag_config.csv` — Button DAG definitions

Defines which button DAGs appear in Airflow UI.

| Column | Description | Example |
|--------|-------------|---------|
| `layer__data_subject__src` | Button DAG ID | `src2brz__accounting__postgres_crm` |
| `schedule_interval` | Cron or `None` (manual only) | `None` |
| `trigger_target` | DAG to trigger on click | `coordinator` |
| `tags` | Comma-separated Airflow tags | `button,src2brz,accounting` |
| `description` | DAG description in UI | `Button: trigger source-to-bronze...` |
| `dag_active` | `1` = generate, `0` = skip | `1` |

### `layer_management_config.csv` — Auto-trigger rules

Controls whether completing one layer automatically triggers the next.

| Column | Description | Example |
|--------|-------------|---------|
| `source_layer` | Layer that just completed | `src2brz` |
| `target_layer` | Layer to trigger next | `brz2sil` |
| `data_subject` | Business domain | `accounting` |
| `source` | Source system | `postgres_crm` |
| `auto_trigger` | `1` = auto-trigger, `0` = manual | `1` |
| `active` | `1` = rule active, `0` = disabled | `1` |

---

## 4. Adding a New Source

**Scenario:** You have a new PostgreSQL database `postgres_hr` with HR data and you want to ingest the `employee` and `department` tables.

### Step 1: Add source credentials

Edit `.dlt/secrets.toml`:
```toml
[sources.postgres_hr]
credentials = "postgresql://user:pass@host:5432/hr_db"
```

### Step 2: Add tables to `config/src2brz_config.csv`

```csv
52,employee,postgres_hr,public,hr,incremental,write_date,1/1/2024,id,10,1
53,department,postgres_hr,public,hr,full,write_date,1/1/2024,id,10,1
```

Key decisions:
- **data_subject**: `hr` — this groups all HR-related tables and determines the staging schema (`stg__hr__postgres_hr`)
- **load_strategy**: `incremental` for tables with a reliable timestamp column, `full` for small reference tables
- **load_sequence**: `10` for both (no dependency order needed between these tables)

### Step 3: Add same tables to `config/brz2sil_config.csv`

Same rows as Step 2. Both configs must stay in sync.

### Step 4: Add button DAGs to `config/dag_config.csv`

```csv
7,src2brz__hr__postgres_hr,None,2024-01-01,1,False,0,5,coordinator,"button,src2brz,hr,postgres_hr",Button: trigger source-to-bronze for hr from postgres_hr,1
8,brz2sil__hr__postgres_hr,None,2024-01-01,1,False,0,5,coordinator,"button,brz2sil,hr,postgres_hr",Button: trigger bronze-to-silver for hr from postgres_hr,1
```

### Step 5: Add auto-trigger rule to `config/layer_management_config.csv`

```csv
src2brz,brz2sil,hr,postgres_hr,1,1
```

### Step 6: Regenerate button DAGs

Run the DAG generator inside the Airflow container:
```bash
python /opt/airflow/dags/dag_init_script.py
```

Or wait for the `dag_generator` DAG to run (every 5 minutes).

### Step 7: Update stg2sil Dataset schedule (if needed)

If you want stg2sil to wait for this new source before running dbt, add the Dataset to `dags/layer__execution/stg2sil__process_whdata.py`:

```python
schedule=[
    Dataset("stg__accounting__postgres_crm"),
    Dataset("stg__accounting__postgres_timesheet"),
    Dataset("stg__hr__postgres_hr"),  # new
],
```

### Step 8: Test

1. Trigger `src2brz__hr__postgres_hr` from Airflow UI
2. Check parquet files appear in `/data/bronze/hr/postgres_hr/public/employee/` and `/department/`
3. Auto-trigger should fire brz2sil (if configured in Step 5)
4. Check warehouse schema `stg__hr__postgres_hr` has `employee` and `department` tables

---

## 5. Adding New Tables to an Existing Source

**Scenario:** Add `account_move` to the existing `postgres_crm` source.

### Step 1: Add row to `config/src2brz_config.csv`

```csv
52,account_move,postgres_crm,public,accounting,incremental,write_date,1/1/2024,id,10,1
```

### Step 2: Add same row to `config/brz2sil_config.csv`

Same row as Step 1.

### Step 3: Done

No DAG changes needed. The existing `src2brz__accounting__postgres_crm` button will now extract `account_move` along with all other tables in the config for that (data_subject, source) pair. Next run will pick up the new table automatically.

---

## 6. Adding a New dbt Model (Silver Layer)

**Scenario:** Create a silver model `silver__accounting__account_tax` that deduplicates `account_tax` from both `postgres_crm` and `postgres_timesheet`.

### Step 1: Declare sources in `dbt/models/silver/_silver_sources.yml`

Add the table to existing sources (if not already declared):

```yaml
sources:
  - name: stg__accounting__postgres_crm
    schema: stg__accounting__postgres_crm
    tables:
      - name: account_account
      - name: account_tax          # add this

  - name: stg__accounting__postgres_timesheet
    schema: stg__accounting__postgres_timesheet
    tables:
      - name: account_account
      - name: account_tax          # add this
```

### Step 2: Create the model SQL

Create `dbt/models/silver/silver__accounting__account_tax.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key=['id', '_source'],
    schema='sil__accounting',
    on_schema_change='append_new_columns'
) }}

{{ silver_dedup(
    ['stg__accounting__postgres_crm', 'stg__accounting__postgres_timesheet'],
    'account_tax'
) }}
```

The `silver_dedup` macro handles:
- Unioning data from multiple sources
- Adding a `_source` column (e.g. `postgres_crm`, `postgres_timesheet`)
- Deduplicating by `(id, _source)` keeping the latest `_dlt_load_id`
- Incremental logic (only new rows since last load)

### Step 3: Add tests in `dbt/models/silver/_silver_models.yml`

```yaml
  - name: silver__accounting__account_tax
    description: "Deduplicated tax configuration merged from multiple Odoo sources."
    tests:
      - unique:
          column_name: "(id || '_' || _source)"
    columns:
      - name: id
        tests:
          - not_null
      - name: _source
        tests:
          - not_null
```

### Step 4: Test locally

```bash
cd dbt
dbt run --select silver__accounting__account_tax
dbt test --select silver__accounting__account_tax
```

### Step 5: Done

The stg2sil DAG will automatically include this model in future runs (it runs `dbt run --select silver` which picks up all models in the `silver/` directory).

---

## 7. Creating a dbt Snapshot (SCD-2)

**Scenario:** Track historical changes to `account_account` using Slowly Changing Dimension Type 2.

### Step 1: Create snapshot file

Create `dbt/snapshots/silver__accounting__dim_account.sql`:

```sql
{% snapshot silver__accounting__dim_account %}

{{ config(
    target_schema='sil__accounting',
    unique_key='id',
    strategy='timestamp',
    updated_at='write_date',
) }}

select *
from {{ source('stg__accounting__postgres_timesheet', 'account_account') }}

{% endsnapshot %}
```

### Step 2: Done

The stg2sil DAG runs `dbt snapshot` as part of its task chain (`run_dbt_stg → run_dbt_snapshot → run_dbt_silver → test_dbt`), so snapshots execute automatically between stg and silver models.

---

## 8. Disabling/Enabling Tables

### Disable a table (stop extracting)

Set `table_load_active` to `0` in both `src2brz_config.csv` and `brz2sil_config.csv`:

```csv
# Before
5,account_incoterms,postgres_crm,public,accounting,incremental,write_date,1/1/2024,id,10,1

# After
5,account_incoterms,postgres_crm,public,accounting,incremental,write_date,1/1/2024,id,10,0
```

The `process_object` DAG filters out inactive tables before triggering execution. No restart needed — takes effect on next run.

### Disable auto-trigger between layers

Set `auto_trigger` to `0` in `layer_management_config.csv`:

```csv
# Before
src2brz,brz2sil,accounting,postgres_crm,1,1

# After — src2brz completes but does NOT auto-trigger brz2sil
src2brz,brz2sil,accounting,postgres_crm,0,1
```

### Disable a button DAG

Set `dag_active` to `0` in `dag_config.csv`, then rerun `dag_init_script.py`. The button DAG file will be deleted from `dags/layer__data_subject__src/`.

---

## 9. Changing Load Strategy

### Switch from incremental to full load

Edit `src2brz_config.csv`:

```csv
# Before — incremental (only new rows since last cursor value)
1,account_account,postgres_crm,public,accounting,incremental,write_date,1/1/2024,id,10,1

# After — full (reload entire table every run)
1,account_account,postgres_crm,public,accounting,full,write_date,1/1/2024,id,10,1
```

Note: On the first run of any table, dlt always does a full load regardless of `load_strategy`. The strategy only applies on subsequent runs.

### Clear pipeline state for a fresh start

If you need to force a full re-extraction (e.g. after a schema change in source):

```bash
# Inside Airflow container:

# Clear dlt local state for a specific source
rm -rf ~/.dlt/pipelines/bronze_postgres_crm_accounting

# Clear destination state
rm -rf /opt/airflow/data/bronze/accounting/postgres_crm/public/{table}/_dlt_pipeline_state

# For warehouse (stg layer):
rm -rf ~/.dlt/pipelines/stg_postgres_crm_accounting
```

---

## 10. System Architecture Details

### How Button DAGs Are Generated

1. `dag_config.csv` defines what buttons should exist
2. `dag_generator` DAG runs every 5 minutes, executing `dag_init_script.py`
3. The script:
   - Clears existing button DAGs in `dags/layer__data_subject__src/`
   - Reads `dag_config.csv`
   - Generates a Python file per active row using a template
4. Each generated button DAG simply triggers the `coordinator` with `conf={"button": DAG_ID}`

### How the Coordinator Routes

1. Receives `conf={"button": "src2brz__accounting__postgres_crm"}`
2. Splits on `__` → `layer=src2brz`, `data_subject=accounting`, `source=postgres_crm`
3. Triggers `{layer}_get_config` (single underscore) → `src2brz_get_config`
4. Passes `{button, layer, data_subject, source}` as conf

### How Config Flows Through the Pipeline

```
coordinator
  conf: {button, layer, data_subject, source}
       ↓
get_config (reads CSV, filters by data_subject + source)
  conf: {button, layer, data_subject, source, tables: [...]}
       ↓
process_object (filters active, sorts by load_sequence)
  conf: {button, layer, data_subject, source, tables: [sorted active only]}
       ↓
execution DAG (processes all tables in the payload)
```

### How Auto-Trigger Works (src2brz → brz2sil)

1. `src2brz_rdbms2parquet_ingestion` has a final task `trigger_next_layer`
2. It calls `get_next_layer("src2brz", data_subject, source)` from `layer_management.py`
3. `layer_management.py` reads `layer_management_config.csv` and checks:
   - source_layer matches, data_subject matches, source matches, active=1, auto_trigger=1
4. If match found: returns `"brz2sil__{data_subject}__{source}"` (the button name)
5. `trigger_next_layer` calls `trigger_dag("coordinator", conf={"button": button_name})`
6. This starts the brz2sil flow through the coordinator

### How stg2sil Is Triggered (Dataset Events)

1. `brz2stg_parquet2postgres_ingestion` emits a `Dataset("stg__{subject}__{source}")` event after loading
2. `stg2sil__process_whdata` is scheduled on multiple Datasets:
   ```python
   schedule=[
       Dataset("stg__accounting__postgres_crm"),
       Dataset("stg__accounting__postgres_timesheet"),
   ]
   ```
3. Airflow triggers stg2sil only after ALL datasets in the list have been updated
4. stg2sil then runs: dbt stg models → snapshots → silver models → tests

### How the Audit System Works

Every Airflow task callable is wrapped with `@audited`:
- **Before execution**: records start time, sets DagRun note in Airflow UI
- **On success**: logs to file + database with status=success
- **On failure**: logs error traceback, then re-raises the exception

Audit destinations:
- File: `/opt/airflow/logs/audit/{dag_id}/{YYYY-MM-DD}.log`
- Database: `meta.pipeline_audit` in warehouse (auto-created)
- Airflow UI: DagRun note (visible in Grid view)

### How dbt Schemas Are Determined

1. **Staging schemas** (created by dlt): `stg__{data_subject}__{source_name}`
   - Determined by `build_stg_pipeline()` in `src/ingestion/stg.py`
2. **Silver schemas** (created by dbt): controlled by model `config(schema=...)`
   - Default: `sil` (from `dbt_project.yml`)
   - Override per model: `schema='sil__accounting'`
3. The `generate_schema_name` macro uses `custom_schema_name` directly (no prefix concatenation)

---

## 11. Monitoring

### Row Counts

Row counts are automatically captured at each layer:
- **src2brz**: `write_parquet` task extracts row counts from dlt `load_info` and returns them
- **brz2stg**: `load_to_warehouse` task captures per-table row counts from dlt
- Row counts are logged to both file audit (`rows=N`) and `meta.pipeline_audit.row_count`

Query row counts:
```sql
SELECT dag_id, task_id, source, data_subject, row_count, finished_at
FROM meta.pipeline_audit
WHERE row_count IS NOT NULL
ORDER BY finished_at DESC;
```

### Data Freshness

The `data_freshness_check` DAG runs daily at 8 AM UTC and checks whether each source has been loaded within its configured threshold.

Configure thresholds in `config/freshness_config.csv`:
```csv
source_name,data_subject,max_stale_hours,active
postgres_crm,accounting,24,1
postgres_timesheet,accounting,24,1
maria_erp,project,48,1
```

If a source exceeds its threshold, an email alert is sent to recipients configured in `config/alert_config.csv`.

### Email Alerting

Alerts are sent for 3 event types:
- **pipeline_failure**: Any execution DAG task failure (via Airflow `on_failure_callback`)
- **data_freshness**: Stale data detected by freshness check
- **dbt_test_failure**: dbt tests fail in stg2sil

Configure recipients in `config/alert_config.csv`:
```csv
alert_type,recipients,active
pipeline_failure,admin@example.com,1
data_freshness,admin@example.com,1
dbt_test_failure,admin@example.com,1
```

Multiple recipients: separate with `;` (e.g. `alice@co.com;bob@co.com`).

SMTP setup: Set environment variables in `.env` (see `.env.example`):
```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_MAIL_FROM=your-email@gmail.com
```

If SMTP is not configured, alerts are logged to stdout instead of being emailed.

### Data Quality (dbt Test Results)

dbt test results are parsed from `dbt/target/run_results.json` and stored in `meta.dbt_test_results`:

```sql
SELECT test_name, status, failures, execution_time, created_at
FROM meta.dbt_test_results
ORDER BY created_at DESC;

-- Failure rate over time
SELECT DATE(created_at),
       COUNT(*) FILTER (WHERE status = 'pass') as passed,
       COUNT(*) FILTER (WHERE status = 'fail') as failed
FROM meta.dbt_test_results
GROUP BY DATE(created_at)
ORDER BY 1 DESC;
```

Failed tests trigger an email alert to `dbt_test_failure` recipients.

---

## 12. Quick Reference: File Locations

### Configuration
| File | Purpose |
|------|---------|
| `config/src2brz_config.csv` | Source extraction table definitions |
| `config/brz2sil_config.csv` | Bronze-to-silver table definitions |
| `config/dag_config.csv` | Button DAG definitions |
| `config/layer_management_config.csv` | Auto-trigger rules between layers |
| `config/freshness_config.csv` | Data freshness thresholds per source |
| `config/alert_config.csv` | Email alert recipients per alert type |
| `.dlt/secrets.toml` | Database credentials |

### DAGs
| File | DAG ID |
|------|--------|
| `dags/coordinator.py` | `coordinator` |
| `dags/layer__get_config/src2brz__get_config.py` | `src2brz_get_config` |
| `dags/layer__get_config/brz2sil__get_config.py` | `brz2sil_get_config` |
| `dags/layer__process_object/src2brz__process_object.py` | `src2brz__process_object` |
| `dags/layer__process_object/brz2sil__process_object.py` | `brz2sil__process_object` |
| `dags/layer__execution/src2brz__rdbms2parquet_ingestion.py` | `src2brz_rdbms2parquet_ingestion` |
| `dags/layer__execution/brz2stg__parquet2postgres_ingestion.py` | `brz2stg_parquet2postgres_ingestion` |
| `dags/layer__execution/stg2sil__process_whdata.py` | `stg2sil__process_whdata` |
| `dags/dag_generator.py` | `dag_generator` |
| `dags/dag_init_script.py` | Button DAG generation script |
| `dags/monitoring/data_freshness_check.py` | `data_freshness_check` |

### Source Code
| File | Purpose |
|------|---------|
| `src/ingestion/config.py` | CSV config loading and dataclasses |
| `src/ingestion/bronze.py` | dlt extraction (RDBMS → parquet) |
| `src/ingestion/stg.py` | dlt loading (parquet → warehouse) |
| `src/ingestion/stg_cli.py` | dbt CLI wrappers |
| `src/ingestion/layer_management.py` | Auto-trigger logic |
| `src/ingestion/alert.py` | Email alerting utility |
| `src/ingestion/audit/decorator.py` | `@audited` decorator (extracts row counts) |
| `src/ingestion/audit/db_logger.py` | Writes to `meta.pipeline_audit` and `meta.dbt_test_results` |

### dbt
| File | Purpose |
|------|---------|
| `dbt/dbt_project.yml` | Project config, default schemas |
| `dbt/models/silver/_silver_sources.yml` | Source declarations for silver models |
| `dbt/models/silver/_silver_models.yml` | Silver model tests |
| `dbt/macros/silver_dedup.sql` | Multi-source dedup macro |
| `dbt/macros/generate_schema_name.sql` | Schema name override |
