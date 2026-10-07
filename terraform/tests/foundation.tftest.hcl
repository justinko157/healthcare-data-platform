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
  # Explicit, so a local terraform.tfvars (adopting a live account) never leaks into tests.
  adopt_existing_account = false
}

run "foundation" {
  command = plan

  assert {
    condition     = snowflake_warehouse.hdp.name == "HDP_WH" && snowflake_warehouse.hdp.warehouse_size == "XSMALL" && snowflake_warehouse.hdp.auto_suspend == 60
    error_message = "HDP_WH must be XSMALL with auto_suspend 60."
  }

  assert {
    condition     = snowflake_database.healthcare.name == "HEALTHCARE"
    error_message = "Database must be HEALTHCARE."
  }

  assert {
    condition     = toset(keys(snowflake_schema.this)) == toset(["RAW", "STAGING", "INTERMEDIATE", "MARTS", "SECURITY"])
    error_message = "Exactly the five pipeline schemas."
  }

  assert {
    condition     = local.schema_fqn["MARTS"] == "\"HEALTHCARE\".\"MARTS\""
    error_message = "schema_fqn must be the quoted fully qualified name."
  }

  assert {
    condition     = toset(keys(snowflake_account_role.this)) == toset(["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER", "PLATFORM_ADMIN"])
    error_message = "Exactly the five pipeline roles."
  }

  assert {
    condition     = alltrue([for g in snowflake_grant_account_role.to_sysadmin : g.parent_role_name == "SYSADMIN"])
    error_message = "Every pipeline role rolls up to SYSADMIN."
  }
}
