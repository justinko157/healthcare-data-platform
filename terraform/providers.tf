# Two aliases, so each object is owned by the role Snowflake recommends for it:
# SYSADMIN owns the warehouse, database and schemas; SECURITYADMIN owns roles, users and grants.
# Neither is ACCOUNTADMIN (tests/test_terraform.py enforces that).
locals {
  terraform_private_key = file(var.terraform_private_key_path)
}

provider "snowflake" {
  alias             = "sysadmin"
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.terraform_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = local.terraform_private_key
  role              = "SYSADMIN"
}

provider "snowflake" {
  alias             = "securityadmin"
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.terraform_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = local.terraform_private_key
  role              = "SECURITYADMIN"
}
