locals {
  # PLATFORM_ADMIN is left out on purpose: creating masking policies is DDL and needs no warehouse.
  warehouse_users = ["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER"]

  # "<ROLE>/<SCHEMA>" => privileges on that schema.
  # ANALYST and PHI_READER get only USAGE on MARTS: SELECT on mart tables is granted by dbt's
  # secure_model post-hook after masking policies are attached. Never add SELECT or future
  # grants on MARTS here (tests/grants.tftest.hcl enforces it).
  schema_grants = {
    "LOADER/RAW"               = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
    "TRANSFORMER/RAW"          = ["USAGE"]
    "TRANSFORMER/STAGING"      = ["USAGE", "CREATE VIEW"]
    "TRANSFORMER/INTERMEDIATE" = ["USAGE", "CREATE VIEW"]
    "TRANSFORMER/MARTS"        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
    "TRANSFORMER/SECURITY"     = ["USAGE"]
    "ANALYST/MARTS"            = ["USAGE"]
    "PHI_READER/MARTS"         = ["USAGE"]
    "PLATFORM_ADMIN/SECURITY"  = ["USAGE", "CREATE MASKING POLICY"]
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = toset(local.warehouse_users)
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.hdp.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = toset(local.roles)
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.healthcare.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema" {
  for_each          = local.schema_grants
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[split("/", each.key)[0]].name
  privileges        = each.value

  on_schema {
    schema_name = local.schema_fqn[split("/", each.key)[1]]
  }
}

# TRANSFORMER reads every RAW table: those that exist now (all) and those the loader creates later (future).
resource "snowflake_grant_privileges_to_account_role" "transformer_raw_all_tables" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this["TRANSFORMER"].name
  privileges        = ["SELECT"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.schema_fqn["RAW"]
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_future_tables" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this["TRANSFORMER"].name
  privileges        = ["SELECT"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.schema_fqn["RAW"]
    }
  }
}
