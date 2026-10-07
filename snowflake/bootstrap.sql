-- One-time account setup. Run by a human in Snowsight as ACCOUNTADMIN (README "Snowflake mode").
-- Before running, replace <PUBLIC_KEY> (from scripts/snowflake_keygen.sh) and <YOUR_LOGIN_NAME>.
-- Re-runnable: every statement is IF NOT EXISTS or an idempotent GRANT.
use role accountadmin;

-- Compute and storage
create warehouse if not exists HDP_WH
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true;
create database if not exists HEALTHCARE;
create schema if not exists HEALTHCARE.RAW;
create schema if not exists HEALTHCARE.STAGING;
create schema if not exists HEALTHCARE.INTERMEDIATE;
create schema if not exists HEALTHCARE.MARTS;
create schema if not exists HEALTHCARE.SECURITY;

-- Roles (least privilege), rolled up to SYSADMIN per Snowflake best practice
create role if not exists LOADER;          -- Airflow ingest: writes RAW only
create role if not exists TRANSFORMER;     -- dbt: reads RAW, builds STAGING/INTERMEDIATE/MARTS
create role if not exists ANALYST;         -- reads MARTS; PII masked
create role if not exists PHI_READER;      -- reads MARTS unmasked
create role if not exists PLATFORM_ADMIN;  -- owns masking policies
grant role LOADER to role SYSADMIN;
grant role TRANSFORMER to role SYSADMIN;
grant role ANALYST to role SYSADMIN;
grant role PHI_READER to role SYSADMIN;
grant role PLATFORM_ADMIN to role SYSADMIN;

-- Everyone needs the warehouse and the database
grant usage on warehouse HDP_WH to role LOADER;
grant usage on warehouse HDP_WH to role TRANSFORMER;
grant usage on warehouse HDP_WH to role ANALYST;
grant usage on warehouse HDP_WH to role PHI_READER;
grant usage on warehouse HDP_WH to role PLATFORM_ADMIN;
grant usage on database HEALTHCARE to role LOADER;
grant usage on database HEALTHCARE to role TRANSFORMER;
grant usage on database HEALTHCARE to role ANALYST;
grant usage on database HEALTHCARE to role PHI_READER;
grant usage on database HEALTHCARE to role PLATFORM_ADMIN;

-- LOADER: RAW only
grant usage, create table, create stage on schema HEALTHCARE.RAW to role LOADER;

-- TRANSFORMER: read RAW, build the modeled schemas, reference policies
grant usage on schema HEALTHCARE.RAW to role TRANSFORMER;
grant select on all tables in schema HEALTHCARE.RAW to role TRANSFORMER;
grant select on future tables in schema HEALTHCARE.RAW to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.STAGING to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.INTERMEDIATE to role TRANSFORMER;
grant usage, create table, create view on schema HEALTHCARE.MARTS to role TRANSFORMER;
grant usage on schema HEALTHCARE.SECURITY to role TRANSFORMER;

-- ANALYST and PHI_READER: MARTS schema usage only. Table-level SELECT is granted by dbt's
-- secure_model() post-hook AFTER masking policies are attached, so no unmasked table is ever
-- readable. Do not add FUTURE grants on MARTS here.
grant usage on schema HEALTHCARE.MARTS to role ANALYST;
grant usage on schema HEALTHCARE.MARTS to role PHI_READER;

-- PLATFORM_ADMIN: creates and updates masking policies
grant usage, create masking policy on schema HEALTHCARE.SECURITY to role PLATFORM_ADMIN;

-- Service user for Airflow and dbt. Key-pair auth: Snowflake blocks password-only service users.
create user if not exists HDP_SERVICE
    type = service
    default_warehouse = HDP_WH
    default_role = LOADER
    default_secondary_roles = ()
    rsa_public_key = '<PUBLIC_KEY>';
-- New users default to secondary roles ALL, which would let a LOADER connection also use
-- TRANSFORMER and PLATFORM_ADMIN privileges. Repeated as ALTER so a re-run fixes an existing user.
alter user HDP_SERVICE set default_secondary_roles = ();
grant role LOADER to user HDP_SERVICE;
grant role TRANSFORMER to user HDP_SERVICE;
grant role PLATFORM_ADMIN to user HDP_SERVICE;

-- Your own login, for the masked vs unmasked demo
grant role ANALYST to user <YOUR_LOGIN_NAME>;
grant role PHI_READER to user <YOUR_LOGIN_NAME>;
