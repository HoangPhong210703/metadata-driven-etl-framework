{% snapshot silver__customer__dim_customer %}

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
from {{ source('stg__postgres_crm__customer', 'res_partner') }}

{% endsnapshot %}
