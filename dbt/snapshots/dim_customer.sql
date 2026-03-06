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
from {{ source('stg', 'stg_res_partner') }}

{% endsnapshot %}
