{% macro silver_dedup(sources, table_name, unique_key='id') %}
{% if sources is string %}{% set sources = [sources] %}{% endif %}

with unioned as (
    {% for src in sources %}
    select
        '{{ src.split("__") | last }}' as _source,
        *
    from {{ source(src, table_name) }}
    {% if is_incremental() %}
    where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select distinct on ({{ unique_key }}, _source)
    *
from unioned
order by {{ unique_key }}, _source, _dlt_load_id desc

{% endmacro %}
