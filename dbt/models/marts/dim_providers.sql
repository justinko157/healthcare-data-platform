select
    provider_id,
    provider_name,
    gender,
    specialty,
    organization_id,
    state
from {{ ref('stg_providers') }}
