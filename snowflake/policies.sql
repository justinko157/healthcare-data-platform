-- Masking policies. Applied every pipeline run by the apply_security task as PLATFORM_ADMIN.
-- CREATE ... IF NOT EXISTS plus ALTER ... SET BODY (not CREATE OR REPLACE): Snowflake refuses to
-- replace a policy that is attached to a column, and these stay attached between runs.
-- The exact primary role is checked, so the role hierarchy and secondary roles can't unmask.
-- No semicolons or double dashes inside statements: ingest/security.py splits on them.
use schema HEALTHCARE.SECURITY;

create masking policy if not exists mask_pii_string as (val string) returns string -> '***MASKED***';
alter masking policy mask_pii_string set body ->
    case
        when val is null then null
        when current_role() = 'PHI_READER' then val
        else '***MASKED***'
    end;

create masking policy if not exists mask_date_to_year as (val date) returns date -> null;
alter masking policy mask_date_to_year set body ->
    case
        when current_role() = 'PHI_READER' then val
        else date_from_parts(year(val), 1, 1)
    end;

create masking policy if not exists mask_patient_id as (val string) returns string -> null;
alter masking policy mask_patient_id set body ->
    case
        when current_role() = 'PHI_READER' then val
        else sha2(val, 256)
    end;

grant apply on masking policy mask_pii_string to role TRANSFORMER;
grant apply on masking policy mask_date_to_year to role TRANSFORMER;
grant apply on masking policy mask_patient_id to role TRANSFORMER;
