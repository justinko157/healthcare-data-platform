-- One row: the batch these marts were built from. record_metrics checks it before capturing KPIs,
-- because the marts are shared and another run may rebuild them after this run's dbt_build.
-- The depends_on lines make dbt build this last, after every mart it vouches for. The check and
-- the KPI reads are separate queries, so a much narrower race (a rebuild in between) remains.
-- depends_on: {{ ref('fct_encounters') }}
-- depends_on: {{ ref('dim_patients') }}
-- depends_on: {{ ref('dim_providers') }}
-- depends_on: {{ ref('fct_readmissions_30d') }}
-- depends_on: {{ ref('agg_daily_utilization') }}
select
    cast({{ batch_filter('patients') }} as varchar) as batch_id,
    {{ dbt.current_timestamp() }} as built_at
