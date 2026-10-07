{#- The batch this run builds: var('batch_id') from the pipeline, else the latest loaded batch. -#}
{% macro batch_filter(table) -%}
    {%- set batch_id = var('batch_id', none) -%}
    {%- if batch_id -%}
        '{{ batch_id }}'
    {%- else -%}
        (select max(_batch_id) from {{ source('raw', table) }})
    {%- endif -%}
{%- endmacro %}
