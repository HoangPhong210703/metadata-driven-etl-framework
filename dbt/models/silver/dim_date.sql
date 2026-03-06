{{ config(materialized='table', schema='silver') }}

with date_spine as (
    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as date
)

select
    cast(to_char(date, 'YYYYMMDD') as int)  as date_key,
    date,
    extract(year from date)::int             as year,
    extract(month from date)::int            as month,
    extract(quarter from date)::int          as quarter,
    extract(isodow from date)::int           as day_of_week,
    extract(isodow from date) >= 6           as is_weekend
from date_spine
