mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TESTUSER"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
}

run "privilege_matrix" {
  command = plan

  # Masking invariant: analysts get only USAGE on MARTS; table SELECT comes from dbt's
  # secure_model post-hook, after masking is attached.
  assert {
    condition     = snowflake_grant_privileges_to_account_role.schema["ANALYST/MARTS"].privileges == toset(["USAGE"]) && snowflake_grant_privileges_to_account_role.schema["PHI_READER/MARTS"].privileges == toset(["USAGE"])
    error_message = "ANALYST and PHI_READER must have only USAGE on MARTS."
  }

  assert {
    condition     = length([for k, v in local.schema_grants : k if contains(v, "SELECT")]) == 0
    error_message = "No schema-level SELECT grants."
  }

  # The only schema-object (all/future) grants are TRANSFORMER's reads of RAW.
  assert {
    condition = (
      snowflake_grant_privileges_to_account_role.transformer_raw_future_tables.account_role_name == "TRANSFORMER" &&
      snowflake_grant_privileges_to_account_role.transformer_raw_future_tables.on_schema_object[0].future[0].in_schema == "\"HEALTHCARE\".\"RAW\"" &&
      snowflake_grant_privileges_to_account_role.transformer_raw_all_tables.on_schema_object[0].all[0].in_schema == "\"HEALTHCARE\".\"RAW\""
    )
    error_message = "Future/all table grants exist only for TRANSFORMER on RAW."
  }

  # Spec section 5 cut: view-only layers get no CREATE TABLE.
  assert {
    condition     = !contains(local.schema_grants["TRANSFORMER/STAGING"], "CREATE TABLE") && !contains(local.schema_grants["TRANSFORMER/INTERMEDIATE"], "CREATE TABLE")
    error_message = "TRANSFORMER must not get CREATE TABLE on STAGING or INTERMEDIATE."
  }

  # Spec section 5 cut: policy DDL needs no warehouse.
  assert {
    condition     = !contains(local.warehouse_users, "PLATFORM_ADMIN")
    error_message = "PLATFORM_ADMIN gets no warehouse usage."
  }

  assert {
    condition     = toset(keys(snowflake_grant_privileges_to_account_role.database_usage)) == toset(local.roles)
    error_message = "Every pipeline role gets USAGE on HEALTHCARE."
  }

  assert {
    condition     = snowflake_grant_privileges_to_account_role.schema["LOADER/RAW"].privileges == toset(["USAGE", "CREATE TABLE", "CREATE STAGE"])
    error_message = "LOADER writes RAW: USAGE, CREATE TABLE, CREATE STAGE."
  }
}
