with source as (
    select * from {{ source('raw', 'providers') }}
    where _batch_id = {{ batch_filter('providers') }}
)

select
    "ID"           as provider_id,
    "ORGANIZATION" as organization_id,
    "NAME"         as provider_name,
    "GENDER"       as gender,
    "SPECIALITY"   as specialty,
    "STATE"        as state
from source
