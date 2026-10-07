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

run "users" {
  command = plan

  # Otherwise a LOADER connection would also carry TRANSFORMER and PLATFORM_ADMIN privileges.
  assert {
    condition     = snowflake_service_user.hdp_service.default_secondary_roles_option == "NONE"
    error_message = "HDP_SERVICE must have secondary roles off."
  }

  assert {
    condition     = snowflake_service_user.hdp_service.rsa_public_key == "AAAAdummyBBBBdummy"
    error_message = "The public key must be the PEM body on one line, without header/footer."
  }

  assert {
    condition     = toset([for g in snowflake_grant_account_role.service : g.role_name]) == toset(["LOADER", "TRANSFORMER", "PLATFORM_ADMIN"]) && alltrue([for g in snowflake_grant_account_role.service : g.user_name == "HDP_SERVICE"])
    error_message = "HDP_SERVICE holds exactly LOADER, TRANSFORMER and PLATFORM_ADMIN."
  }

  assert {
    condition     = toset([for g in snowflake_grant_account_role.human : g.role_name]) == toset(["ANALYST", "PHI_READER"]) && alltrue([for g in snowflake_grant_account_role.human : g.user_name == "TESTUSER"])
    error_message = "The human user gets ANALYST and PHI_READER, by its upper-case name."
  }
}
