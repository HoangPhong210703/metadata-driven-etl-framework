{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg_temp', 'res_partner') }}
order by id, _dlt_load_id desc
