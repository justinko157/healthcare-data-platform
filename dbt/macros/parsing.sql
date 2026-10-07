{#- RAW is all VARCHAR. Synthea writes '' for null and ISO timestamps ending in 'Z' (UTC). -#}
{% macro parse_ts(column) -%}
    cast(nullif(replace({{ column }}, 'Z', ''), '') as timestamp)
{%- endmacro %}

{% macro parse_date(column) -%}
    cast(nullif({{ column }}, '') as date)
{%- endmacro %}

{% macro parse_num(column) -%}
    cast(nullif({{ column }}, '') as decimal(18, 2))
{%- endmacro %}
