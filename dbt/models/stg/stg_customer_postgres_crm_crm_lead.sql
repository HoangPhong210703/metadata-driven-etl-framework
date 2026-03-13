{{ config(materialized='incremental', schema='stg', unique_key='id') }}

select distinct on (id)
    *
from {{ source('stg__postgres_crm__customer', 'crm_lead') }}

{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
{% endif %}
order by id, _dlt_load_id desc
