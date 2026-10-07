{#- A column's meta, whether declared as `meta:` or `config: {meta: ...}` (dbt 1.10 style). -#}
{% macro column_meta(col) -%}
    {%- set merged = {} -%}
    {%- if col.meta -%}{%- do merged.update(col.meta) -%}{%- endif -%}
    {%- if col.config and col.config.meta -%}{%- do merged.update(col.config.meta) -%}{%- endif -%}
    {{ return(merged) }}
{%- endmacro %}
