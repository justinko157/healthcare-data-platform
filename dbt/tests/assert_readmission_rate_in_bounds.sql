-- The readmission rate is a proportion.
select readmission_rate
from (
    select avg(case when is_readmitted then 1.0 else 0.0 end) as readmission_rate
    from {{ ref('fct_readmissions_30d') }}
) as r
where readmission_rate < 0 or readmission_rate > 1
