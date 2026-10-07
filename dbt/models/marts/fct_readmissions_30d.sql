-- One row per inpatient discharge. Readmitted = the patient's next inpatient admission starts
-- within 30 days after this discharge (a simplified CMS-style definition: all causes, no exclusions).
with inpatient as (
    select encounter_id, patient_id, started_at, stopped_at
    from {{ ref('int_encounters_enriched') }}
    where encounter_class = 'inpatient'
      and stopped_at is not null
),

with_next as (
    select
        encounter_id,
        patient_id,
        stopped_at,
        lead(encounter_id) over (partition by patient_id order by started_at, encounter_id) as next_encounter_id,
        lead(started_at)   over (partition by patient_id order by started_at, encounter_id) as next_started_at
    from inpatient
)

select
    encounter_id      as index_encounter_id,
    patient_id,
    stopped_at        as discharged_at,
    case when is_readmitted then next_encounter_id end as readmission_encounter_id,
    case when is_readmitted then next_started_at end   as readmitted_at,
    is_readmitted
from (
    select
        *,
        coalesce(
            next_started_at >= stopped_at
            and next_started_at <= {{ dbt.dateadd('day', 30, 'stopped_at') }},
            false
        ) as is_readmitted
    from with_next
) as flagged
