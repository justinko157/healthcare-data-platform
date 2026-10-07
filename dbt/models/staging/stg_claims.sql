with source as (
    select * from {{ source('raw', 'claims') }}
    where _batch_id = {{ batch_filter('claims') }}
)

select
    "ID"            as claim_id,
    "PATIENTID"     as patient_id,
    "PROVIDERID"    as provider_id,
    "APPOINTMENTID" as encounter_id,  -- Synthea stores the encounter Id here (checked in tests/test_sample_data.py)
    {{ parse_ts('"SERVICEDATE"') }} as service_at
from source
