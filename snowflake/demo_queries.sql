-- Masked vs unmasked proof. Run in Snowsight as your own login after a successful
-- WAREHOUSE=snowflake pipeline run. Captured outputs: docs/snowflake_demo_output.md.
use secondary roles none;   -- each query sees exactly one role's privileges
use warehouse HDP_WH;

-- 1. ANALYST: identifiers masked, patient_id hashed, birth dates cut to Jan 1.
use role ANALYST;
select patient_id, first_name, last_name, ssn, birth_date, city, state, age_years
from HEALTHCARE.MARTS.DIM_PATIENTS
order by patient_id      -- already the SHA-256 hash for ANALYST
limit 3;

-- 2. PHI_READER: the same three patients, unmasked (ordered by the same hash).
use role PHI_READER;
select patient_id, first_name, last_name, ssn, birth_date, city, state, age_years
from HEALTHCARE.MARTS.DIM_PATIENTS
order by sha2(patient_id, 256)
limit 3;

-- 3. Analysts can still join and count: the hash is the same in every mart.
use role ANALYST;
select count(*) as encounters_joined_to_patients
from HEALTHCARE.MARTS.FCT_ENCOUNTERS e
join HEALTHCARE.MARTS.DIM_PATIENTS p on p.patient_id = e.patient_id;

-- 4. Least privilege: ANALYST cannot see RAW.
--    Expected: an error that RAW "does not exist or not authorized" (ANALYST has no USAGE on
--    the schema, so Snowflake reports it at schema level).
select count(*) from HEALTHCARE.RAW.PATIENTS;
