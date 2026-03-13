{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg__postgres_crm__customer', 'res_partner') }}
order by id, _dlt_load_id desc
