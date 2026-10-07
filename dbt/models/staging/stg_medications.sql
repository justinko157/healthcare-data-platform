with source as (
    select * from {{ source('raw', 'medications') }}
    where _batch_id = {{ batch_filter('medications') }}
)

select
    "PATIENT"     as patient_id,
    "ENCOUNTER"   as encounter_id,
    "PAYER"       as payer_id,
    "CODE"        as medication_code,
    "DESCRIPTION" as medication_description,
    {{ parse_ts('"START"') }} as started_at,
    {{ parse_ts('"STOP"') }}  as stopped_at,
    {{ parse_num('"TOTALCOST"') }} as total_cost
from source
