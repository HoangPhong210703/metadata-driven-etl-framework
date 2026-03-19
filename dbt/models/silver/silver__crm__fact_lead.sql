{{ config(
    materialized='incremental',
    schema='sil__accounting',
    unique_key='lead_id'
) }}

with leads as (
    select
        id          as lead_id,
        partner_id,
        create_date,
        expected_revenue,
        prorated_revenue
    from {{ source('stg__postgres_crm__customer', 'crm_lead') }}
),

customers as (
    select
        dbt_scd_id  as customer_key,
        id          as partner_id
    from {{ ref('silver__customer__dim_customer') }}
    where dbt_valid_to is null
),

dates as (
    select date_key, date
    from {{ ref('silver__common__dim_date') }}
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
