-- Run once in Snowsight as ACCOUNTADMIN (README "Snowflake mode"). Terraform does the rest.
-- Before running, replace <TERRAFORM_PUBLIC_KEY> with the output of:
--   ./scripts/snowflake_keygen.sh terraform
use role accountadmin;

-- ── Part 1: always ──────────────────────────────────────────────────────────────────────
-- The user Terraform logs in as. SYSADMIN owns infrastructure objects and SECURITYADMIN owns
-- roles, users and grants; Terraform never holds ACCOUNTADMIN.
create user if not exists TERRAFORM
    type = service
    default_role = SYSADMIN
    default_secondary_roles = ()
    rsa_public_key = '<TERRAFORM_PUBLIC_KEY>';
grant role SYSADMIN to user TERRAFORM;
grant role SECURITYADMIN to user TERRAFORM;

-- ── Part 2: only for an account first set up with the old snowflake/bootstrap.sql ────────
-- That script created everything as ACCOUNTADMIN, so ACCOUNTADMIN owns it, and SYSADMIN or
-- SECURITYADMIN can't alter it. Move ownership to the roles Terraform uses, keeping existing grants.
-- Skip this whole part on a fresh account.
grant ownership on warehouse HDP_WH to role SYSADMIN copy current grants;
grant ownership on database HEALTHCARE to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.RAW to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.STAGING to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.INTERMEDIATE to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.MARTS to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.SECURITY to role SYSADMIN copy current grants;
grant ownership on role LOADER to role SECURITYADMIN;
grant ownership on role TRANSFORMER to role SECURITYADMIN;
grant ownership on role ANALYST to role SECURITYADMIN;
grant ownership on role PHI_READER to role SECURITYADMIN;
grant ownership on role PLATFORM_ADMIN to role SECURITYADMIN;
grant ownership on user HDP_SERVICE to role SECURITYADMIN;
-- The old script gave PLATFORM_ADMIN warehouse usage it doesn't need. Terraform can't revoke a
-- grant it doesn't describe, so the cut is made here; the Terraform config never grants it.
revoke usage on warehouse HDP_WH from role PLATFORM_ADMIN;
