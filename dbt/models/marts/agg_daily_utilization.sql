select
    encounter_date,
    count(*)                                                          as encounter_count,
    sum(case when encounter_class = 'inpatient' then 1 else 0 end)    as inpatient_count,
    sum(case when encounter_class = 'emergency' then 1 else 0 end)    as emergency_count,
    sum(total_claim_cost)                                             as total_claim_cost
from {{ ref('int_encounters_enriched') }}
group by encounter_date
