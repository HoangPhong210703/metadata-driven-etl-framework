{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg_temp', 'temp_crm_postgres_crm_project_task') }}
order by id, _dlt_load_id desc
