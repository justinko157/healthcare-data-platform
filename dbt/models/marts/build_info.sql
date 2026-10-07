-- One row: the batch these marts were built from. record_metrics checks it before capturing KPIs,
-- because the marts are shared and another run may rebuild them after this run's dbt_build.
select
    cast({{ batch_filter('patients') }} as varchar) as batch_id,
    {{ dbt.current_timestamp() }} as built_at
