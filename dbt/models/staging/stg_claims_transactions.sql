with source as (
    select * from {{ source('raw', 'claims_transactions') }}
    where _batch_id = {{ batch_filter('claims_transactions') }}
)

select
    "ID"        as transaction_id,
    "CLAIMID"   as claim_id,
    "PATIENTID" as patient_id,
    upper("TYPE") as transaction_type,
    {{ parse_num('"AMOUNT"') }} as amount,
    {{ parse_num('"UNITS"') }}  as units
from source
