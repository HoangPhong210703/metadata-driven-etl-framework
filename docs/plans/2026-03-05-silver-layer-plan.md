# Silver Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Silver layer with `dim_customer` (SCD-2), `dim_date` (generated calendar), and `fact_lead` (incremental) using dbt, triggered automatically after the stg DAG.

**Architecture:** dbt reads from the `stg` schema in Postgres. `dim_customer` uses dbt snapshots for SCD-2 history tracking. `dim_date` is a static generated calendar table. `fact_lead` is an incremental model joining both dims. A new `silver_transform` Airflow DAG is triggered by `stg_ingestion` on completion.

**Tech Stack:** dbt-postgres, Airflow `TriggerDagRunOperator`, Postgres `silver` schema.

---

### Task 1: Add `res_partner` to sources.yaml

**Files:**
- Modify: `config/sources.yaml`

`res_partner` is Odoo's customer/partner table — it feeds `dim_customer` through bronze → stg before silver can use it.

**Step 1: Add the table entry**

In `config/sources.yaml`, add under `postgres_crm.tables`:

```yaml
      - name: res_partner
        load_strategy: incremental
        data_subject: crm
        cursor_column: write_date
        initial_value: "2024-01-01"
        primary_key: id
```

**Step 2: Verify config loads without error**

```bash
python -c "from src.config.config import load_sources_config; from pathlib import Path; s = load_sources_config(Path('config/sources.yaml')); print([t.name for src in s for t in src.tables])"
```

Expected output includes `res_partner`.

**Step 3: Add res_partner to stg dbt source definition**

In `dbt/models/stg/_stg_sources.yml`, add under `stg_temp.tables`:

```yaml
      - name: res_partner
      - name: crm_lead
```

**Step 4: Add stg dbt models for new tables**

Create `dbt/models/stg/stg_res_partner.sql`:

```sql
{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg_temp', 'res_partner') }}
order by id, _dlt_load_id desc
```

Create `dbt/models/stg/stg_crm_lead.sql`:

```sql
{{ config(materialized='incremental', schema='stg', unique_key='id') }}

select distinct on (id)
    *
from {{ source('stg_temp', 'crm_lead') }}
order by id, _dlt_load_id desc

{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
{% endif %}
```

**Step 5: Commit**

```bash
git add config/sources.yaml dbt/models/stg/_stg_sources.yml dbt/models/stg/stg_res_partner.sql dbt/models/stg/stg_crm_lead.sql
git commit -m "feat: add res_partner and crm_lead to sources and stg models"
```

---

### Task 2: Configure dbt for Silver schema and snapshots

**Files:**
- Modify: `dbt/dbt_project.yml`
- Create: `dbt/snapshots/` directory

dbt needs to know about the `silver` schema for models and where to find snapshots.

**Step 1: Update dbt_project.yml**

Replace contents of `dbt/dbt_project.yml`:

```yaml
name: etl_warehouse
version: "1.0.0"

profile: warehouse

model-paths: ["models"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  etl_warehouse:
    stg:
      +schema: stg
    silver:
      +schema: silver

snapshots:
  etl_warehouse:
    +target_schema: silver
```

**Step 2: Create snapshots directory**

```bash
mkdir dbt/snapshots
```

**Step 3: Commit**

```bash
git add dbt/dbt_project.yml dbt/snapshots/
git commit -m "feat: configure dbt silver schema and snapshots path"
```

---

### Task 3: Create dim_customer snapshot (SCD-2)

**Files:**
- Create: `dbt/snapshots/dim_customer.sql`

dbt snapshots compare the current state of `stg.res_partner` against previous snapshots and generate SCD-2 records with `dbt_valid_from`, `dbt_valid_to`, `dbt_is_current` columns automatically.

**Step 1: Create the snapshot file**

Create `dbt/snapshots/dim_customer.sql`:

```sql
{% snapshot dim_customer %}

{{
    config(
        target_schema='silver',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date',
    )
}}

select
    id,
    name,
    phone,
    email,
    is_company,
    write_date
from {{ source('stg', 'res_partner') }}

{% endsnapshot %}
```

**Step 2: Add stg source definition for silver models**

Create `dbt/models/silver/_silver_sources.yml`:

```yaml
version: 2

sources:
  - name: stg
    schema: stg
    tables:
      - name: res_partner
      - name: crm_lead
```

**Step 3: Commit**

```bash
git add dbt/snapshots/dim_customer.sql dbt/models/silver/_silver_sources.yml
git commit -m "feat: add dim_customer SCD-2 snapshot"
```

---

### Task 4: Create dim_date model (generated calendar)

**Files:**
- Create: `dbt/models/silver/dim_date.sql`

`dim_date` is generated entirely in SQL using `generate_series` — no source table required. `date_key` uses YYYYMMDD integer format for easy joining with date columns.

**Step 1: Create the model**

Create `dbt/models/silver/dim_date.sql`:

```sql
{{ config(materialized='table', schema='silver') }}

with date_spine as (
    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as date
)

select
    cast(to_char(date, 'YYYYMMDD') as int)  as date_key,
    date,
    extract(year from date)::int             as year,
    extract(month from date)::int            as month,
    extract(quarter from date)::int          as quarter,
    extract(isodow from date)::int           as day_of_week,
    extract(isodow from date) >= 6           as is_weekend
from date_spine
```

**Step 2: Commit**

```bash
git add dbt/models/silver/dim_date.sql
git commit -m "feat: add dim_date generated calendar model"
```

---

### Task 5: Create fact_lead model

**Files:**
- Create: `dbt/models/silver/fact_lead.sql`

`fact_lead` joins `stg.crm_lead` to the current version of `dim_customer` and to `dim_date` via the lead's creation date. Incremental merge on `lead_id` avoids reprocessing the full table each run.

**Step 1: Create the model**

Create `dbt/models/silver/fact_lead.sql`:

```sql
{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='lead_id'
) }}

with leads as (
    select
        id          as lead_id,
        partner_id,
        create_date,
        expected_revenue,
        prorated_revenue
    from {{ source('stg', 'crm_lead') }}

    {% if is_incremental() %}
    where create_date > (select max(create_date) from {{ this }})
    {% endif %}
),

customers as (
    select
        dbt_scd_id  as customer_key,
        id          as partner_id
    from {{ ref('dim_customer') }}
    where dbt_is_current = true
),

dates as (
    select date_key, date
    from {{ ref('dim_date') }}
)

select
    l.lead_id,
    d.date_key,
    c.customer_key,
    l.expected_revenue,
    l.prorated_revenue
from leads l
left join customers c on l.partner_id = c.partner_id
left join dates d on l.create_date::date = d.date
```

**Step 2: Commit**

```bash
git add dbt/models/silver/fact_lead.sql
git commit -m "feat: add fact_lead incremental model"
```

---

### Task 6: Create silver_transform Airflow DAG

**Files:**
- Create: `dags/silver_transform.py`
- Modify: `dags/stg_ingestion.py`

The silver DAG runs two steps in order: snapshot first (to update SCD-2), then models (dim_date + fact_lead). It is triggered by `stg_ingestion` — never scheduled independently.

**Step 1: Create the DAG**

Create `dags/silver_transform.py`:

```python
"""Silver transform DAG — runs dbt snapshots then silver models."""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")


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


def _run_dbt(command: list[str]) -> None:
    credentials = _load_warehouse_credentials()
    _set_dbt_env_vars(credentials)
    result = subprocess.run(
        ["dbt"] + command + ["--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


def run_dbt_snapshots(**kwargs):
    _run_dbt(["snapshot", "--select", "dim_customer"])


def run_dbt_silver(**kwargs):
    _run_dbt(["run", "--select", "silver"])


with DAG(
    dag_id="silver_transform",
    description="Run dbt snapshots and silver models",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["silver", "dbt"],
) as dag:
    snapshot_task = PythonOperator(
        task_id="dbt_snapshot_dim_customer",
        python_callable=run_dbt_snapshots,
    )

    silver_task = PythonOperator(
        task_id="dbt_run_silver",
        python_callable=run_dbt_silver,
    )

    snapshot_task >> silver_task
```

**Step 2: Add TriggerDagRunOperator to stg_ingestion DAG**

In `dags/stg_ingestion.py`, add import:

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
```

Add after the `dbt_task` definition:

```python
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="silver_transform",
    )

    dbt_task >> trigger_silver
```

**Step 3: Commit**

```bash
git add dags/silver_transform.py dags/stg_ingestion.py
git commit -m "feat: add silver_transform DAG and trigger from stg_ingestion"
```

---

### Task 7: Verify end-to-end in Airflow

**Step 1: Reparse DAGs in Airflow UI**

In the Airflow UI, reparse both `stg_ingestion` and `silver_transform` DAGs.

**Step 2: Trigger bronze DAG manually**

Trigger `bronze_ingestion` from the Airflow UI. Verify the chain runs:
`bronze_ingestion` → triggers `stg_ingestion` → triggers `silver_transform`

**Step 3: Check silver schema in Postgres**

Connect to the warehouse Postgres and verify:

```sql
select count(*) from silver.dim_customer;
select count(*) from silver.dim_date;
select count(*) from silver.fact_lead;
```

All three should have rows. `dim_date` should have 3,653 rows (2020–2030).

**Step 4: Verify SCD-2 columns exist on dim_customer**

```sql
select dbt_scd_id, dbt_valid_from, dbt_valid_to, dbt_is_current
from silver.dim_customer
limit 5;
```
