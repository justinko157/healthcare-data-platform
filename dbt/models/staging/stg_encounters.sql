with source as (
    select * from {{ source('raw', 'encounters') }}
    where _batch_id = {{ batch_filter('encounters') }}
)

select
    "ID"                   as encounter_id,
    "PATIENT"              as patient_id,
    "ORGANIZATION"         as organization_id,
    "PROVIDER"             as provider_id,
    "PAYER"                as payer_id,
    lower("ENCOUNTERCLASS") as encounter_class,
    "CODE"                 as encounter_code,
    "DESCRIPTION"          as encounter_description,
    {{ parse_ts('"START"') }} as started_at,
    {{ parse_ts('"STOP"') }}  as stopped_at,
    {{ parse_num('"BASE_ENCOUNTER_COST"') }} as base_encounter_cost,
    {{ parse_num('"TOTAL_CLAIM_COST"') }}    as total_claim_cost,
    {{ parse_num('"PAYER_COVERAGE"') }}      as payer_coverage
from source
