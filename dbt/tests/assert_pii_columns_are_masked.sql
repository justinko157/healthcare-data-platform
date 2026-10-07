{#-
    Fails (one row per problem) when a column analysts can read in MARTS is not protected:
      * a column tagged pii: true with no masking_policy from vars.masking_policies, or
      * a column whose name is tagged pii anywhere in the project (e.g. stg_patients.ssn) but
        isn't protected in the mart (catches forgetting to re-tag after a rename), or
      * a column present in the built table but not documented in YAML (an undocumented
        column cannot be checked, so it fails too).
    Works on DuckDB as well, so CI catches an unprotected field without Snowflake.
-#}
-- depends_on: {{ ref('fct_encounters') }}
-- depends_on: {{ ref('dim_patients') }}
-- depends_on: {{ ref('dim_providers') }}
-- depends_on: {{ ref('fct_readmissions_30d') }}
-- depends_on: {{ ref('agg_daily_utilization') }}
{%- set problems = [] -%}
{%- if execute -%}
    {%- set models = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | list -%}
    {%- set pii_names = [] -%}
    {%- for node in models -%}
        {%- for col in node.columns.values() -%}
            {%- if column_meta(col).get('pii') -%}{%- do pii_names.append(col.name | lower) -%}{%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- for node in models if node.config.schema == 'marts' -%}
        {%- set documented = node.columns.keys() | map('lower') | list -%}
        {%- for col in node.columns.values() -%}
            {%- set meta = column_meta(col) -%}
            {%- if (meta.get('pii') or (col.name | lower) in pii_names)
                   and meta.get('masking_policy') not in var('masking_policies') -%}
                {%- do problems.append(node.name ~ '.' ~ col.name ~ ': PII without a masking policy') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- set relation = adapter.get_relation(node.database, node.schema, node.alias or node.name) -%}
        {%- if relation -%}
            {%- for c in adapter.get_columns_in_relation(relation) -%}
                {%- if (c.name | lower) not in documented -%}
                    {%- do problems.append(node.name ~ '.' ~ (c.name | lower) ~ ': not documented in _marts.yml') -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if problems %}
{%- for p in problems %}
select '{{ p }}' as problem{% if not loop.last %} union all{% endif %}
{%- endfor %}
{%- else %}
select cast(null as varchar) as problem where 1 = 0
{%- endif %}
