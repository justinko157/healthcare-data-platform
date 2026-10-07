with source as (
    select * from {{ source('raw', 'conditions') }}
    where _batch_id = {{ batch_filter('conditions') }}
)

select
    "PATIENT"     as patient_id,
    "ENCOUNTER"   as encounter_id,
    "CODE"        as condition_code,
    "DESCRIPTION" as condition_description,
    {{ parse_date('"START"') }} as started_on,
    {{ parse_date('"STOP"') }}  as stopped_on
from source
