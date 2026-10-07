select
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    p.provider_name,
    p.specialty          as provider_specialty,
    e.payer_id,
    py.payer_name,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.started_at,
    e.stopped_at,
    cast(e.started_at as date) as encounter_date,
    e.total_claim_cost,
    ct.claim_line_total,
    ct.claim_line_count
from {{ ref('stg_encounters') }} as e
left join {{ ref('stg_providers') }} as p on p.provider_id = e.provider_id
left join {{ ref('stg_payers') }} as py on py.payer_id = e.payer_id
left join {{ ref('int_encounter_claim_totals') }} as ct on ct.encounter_id = e.encounter_id
