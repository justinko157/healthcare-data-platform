{#-
    Marts post-hook. On Snowflake, in this order:
      1. attach the masking policy named in each column's meta (dbt just recreated the table,
         which dropped any earlier policies), then
      2. grant SELECT to ANALYST and PHI_READER.
    Until step 2, neither role can read the table, so there is no unmasked window.
    On DuckDB there is no masking; the PII coverage test still checks the declarations.
-#}
{% macro secure_model() -%}
    {%- if execute and target.type == 'snowflake' -%}
        {%- for col in model.columns.values() -%}
            {%- set policy = column_meta(col).get('masking_policy') -%}
            {%- if policy -%}
                {%- do run_query(
                    'alter table ' ~ this ~ ' modify column ' ~ col.name
                    ~ ' set masking policy ' ~ target.database ~ '.SECURITY.' ~ policy ~ ' force'
                ) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- for role in ['ANALYST', 'PHI_READER'] -%}
            {%- do run_query('grant select on table ' ~ this ~ ' to role ' ~ role) -%}
        {%- endfor -%}
    {%- endif -%}
    select 1
{%- endmacro %}
