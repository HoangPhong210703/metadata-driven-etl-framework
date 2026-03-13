{{ config(
    materialized='incremental',
    unique_key='id',
    schema='stg',
    on_schema_change='append_new_columns'
) }}

select distinct on (id)
    *
from {{ source('stg__postgres_timesheet__hr', 'account_account') }}
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
{% endif %}
order by id, _dlt_load_id desc
