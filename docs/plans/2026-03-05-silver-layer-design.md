# Silver Layer Design

## Overview

Transform staging tables into business-ready dimension and fact tables using dbt. The Silver layer sits between Stg (current-state deduplicated tables) and Gold (aggregated reporting tables).

## Data Flow

```
stg schema (Postgres)
    │
    ├─ dbt snapshot  → silver.dim_customer   (SCD-2, full history)
    ├─ dbt model     → silver.dim_date       (generated calendar, no source)
    └─ dbt model     → silver.fact_lead      (joins dims, incremental)
```

## Tables

### dim_customer (SCD-2)

| Column | Type | Description |
|---|---|---|
| `dbt_scd_id` | varchar | Surrogate key (used as `customer_key` in fact) |
| `id` | int | Natural key from `res_partner` |
| `name` | varchar | Customer name |
| `phone` | varchar | Phone number |
| `email` | varchar | Email address |
| `is_company` | boolean | Whether the partner is a company |
| `dbt_valid_from` | timestamp | When this version became active |
| `dbt_valid_to` | timestamp | When this version was superseded (null = current) |
| `dbt_is_current` | boolean | True for the latest version |
| `dbt_updated_at` | timestamp | Source `write_date` at time of snapshot |

- **Source**: `stg.res_partner`
- **dbt type**: `snapshot` with `strategy='timestamp'`, `updated_at='write_date'`, `unique_key='id'`
- **Schema**: `silver`

### dim_date (generated)

| Column | Type | Description |
|---|---|---|
| `date_key` | int | Surrogate key (YYYYMMDD format) |
| `date` | date | Calendar date |
| `year` | int | Year (e.g. 2026) |
| `month` | int | Month number (1–12) |
| `quarter` | int | Quarter (1–4) |
| `day_of_week` | int | Day of week (1=Monday, 7=Sunday) |
| `is_weekend` | boolean | True for Saturday/Sunday |

- **Source**: Generated via dbt date spine macro (no source table)
- **dbt type**: `table`, covers 2020-01-01 to 2030-12-31
- **Schema**: `silver`

### fact_lead

| Column | Type | Description |
|---|---|---|
| `lead_id` | int | Natural key from `crm_lead` |
| `date_key` | int | FK → `dim_date.date_key` (from `create_date`) |
| `customer_key` | varchar | FK → `dim_customer.dbt_scd_id` (current version) |
| `expected_revenue` | numeric | Expected revenue from the lead |
| `prorated_revenue` | numeric | Prorated revenue from the lead |

- **Source**: `stg.crm_lead`
- **dbt type**: `incremental`, merge on `lead_id`
- **Schema**: `silver`
- **Date join**: `create_date::date` → `dim_date.date` → `dim_date.date_key`
- **Customer join**: `partner_id = dim_customer.id WHERE dbt_is_current = true`

## Sources Required

Add `res_partner` to `sources.yaml` under `postgres_crm`:

```yaml
- name: res_partner
  load_strategy: incremental
  data_subject: crm
  cursor_column: write_date
  initial_value: "2024-01-01"
  primary_key: id
```

This flows through bronze → stg before Silver can use it.

## dbt Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── snapshots/
│   └── dim_customer.sql
└── models/
    ├── stg/          (existing)
    └── silver/
        ├── _silver_sources.yml
        ├── dim_date.sql
        └── fact_lead.sql
```

## Airflow Integration

Silver DAG (`silver_transform`) is triggered by the stg DAG completing:

```
bronze_ingestion → stg_ingestion → silver_transform
```

`silver_transform` runs two dbt steps in order:
1. `dbt snapshot --select dim_customer` — build SCD-2 history
2. `dbt run --select silver` — build dim_date and fact_lead

## Key Decisions

- **SCD-2 for dim_customer** — tracks customer attribute changes over time via dbt snapshots
- **dim_date is generated** — no source dependency, static calendar table
- **fact_lead is incremental** — merges on `lead_id` to avoid full reprocessing
- **Silver reads from `stg` schema** — not `stg_temp`, ensuring deduplicated inputs
- **customer_key uses current snapshot** — `WHERE dbt_is_current = true` at join time
