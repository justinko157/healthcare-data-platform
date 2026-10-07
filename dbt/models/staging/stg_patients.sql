with source as (
    select * from {{ source('raw', 'patients') }}
    where _batch_id = {{ batch_filter('patients') }}
)

select
    "ID"        as patient_id,
    "PREFIX"    as name_prefix,
    "FIRST"     as first_name,
    "LAST"      as last_name,
    "SUFFIX"    as name_suffix,
    "MAIDEN"    as maiden_name,
    {{ parse_date('"BIRTHDATE"') }} as birth_date,
    {{ parse_date('"DEATHDATE"') }} as death_date,
    "SSN"       as ssn,
    "DRIVERS"   as drivers_license,
    "PASSPORT"  as passport,
    "ADDRESS"   as address,
    "CITY"      as city,
    "STATE"     as state,
    "COUNTY"    as county,
    "ZIP"       as zip,
    "GENDER"    as gender,
    "RACE"      as race,
    "ETHNICITY" as ethnicity,
    "MARITAL"   as marital_status
from source
