with source as (
    select * from {{ source('raw', 'payers') }}
    where _batch_id = {{ batch_filter('payers') }}
)

select
    "ID"   as payer_id,
    "NAME" as payer_name
from source
