{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg__postgres_crm__crm', 'project_task') }}
order by id, _dlt_load_id desc
