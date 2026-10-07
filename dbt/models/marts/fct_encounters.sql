select
    encounter_id,
    patient_id,
    provider_id,
    provider_name,
    provider_specialty,
    payer_id,
    payer_name,
    encounter_class,
    encounter_code,
    encounter_description,
    started_at,
    stopped_at,
    encounter_date,
    {{ dbt.datediff('started_at', 'stopped_at', 'minute') }} / 60.0 as length_of_stay_hours,
    total_claim_cost,
    claim_line_total,
    claim_line_count
from {{ ref('int_encounters_enriched') }}
