mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TestUser"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
  # Explicit, so a local terraform.tfvars (adopting a live account) never leaks into tests.
  adopt_existing_account = false
}

run "fresh_account_imports_nothing" {
  command = plan

  assert {
    condition     = alltrue([for kind, ids in local.adopt : length(ids) == 0])
    error_message = "With adopt_existing_account = false no import may run (they would fail on a fresh account)."
  }
}

run "adoption_ids_cover_every_managed_instance" {
  command = plan

  assert {
    condition = (
      toset(keys(local.adoption_ids.schemas)) == toset(keys(snowflake_schema.this)) &&
      toset(keys(local.adoption_ids.roles)) == toset(keys(snowflake_account_role.this)) &&
      toset(keys(local.adoption_ids.roles_to_sysadmin)) == toset(keys(snowflake_grant_account_role.to_sysadmin)) &&
      toset(keys(local.adoption_ids.warehouse_usage)) == toset(keys(snowflake_grant_privileges_to_account_role.warehouse_usage)) &&
      toset(keys(local.adoption_ids.database_usage)) == toset(keys(snowflake_grant_privileges_to_account_role.database_usage)) &&
      toset(keys(local.adoption_ids.schema_grants)) == toset(keys(snowflake_grant_privileges_to_account_role.schema)) &&
      toset(keys(local.adoption_ids.service_roles)) == toset(keys(snowflake_grant_account_role.service)) &&
      toset(keys(local.adoption_ids.human_roles)) == toset(keys(snowflake_grant_account_role.human))
    )
    error_message = "Every for_each resource needs an import ID for each instance."
  }

  # The TRANSFORMER cut must appear in the adoption plan: its import IDs list the live
  # privileges (including CREATE TABLE), so plan shows CREATE TABLE being revoked.
  assert {
    condition     = strcontains(local.adoption_ids.schema_grants["TRANSFORMER/STAGING"], "|USAGE,CREATE TABLE,CREATE VIEW|")
    error_message = "Adoption must import what bootstrap.sql granted so plan shows the CREATE TABLE cut."
  }

  assert {
    condition     = local.adoption_ids.human_roles["ANALYST"] == "\"ANALYST\"|USER|\"TESTUSER\""
    error_message = "Human-user grant IDs use the upper-case user name."
  }
}
