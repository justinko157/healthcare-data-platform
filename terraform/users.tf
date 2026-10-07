locals {
  service_roles = ["LOADER", "TRANSFORMER", "PLATFORM_ADMIN"]
  human_roles   = ["ANALYST", "PHI_READER"]

  # Snowflake stores unquoted user names in upper case.
  human_user_name = upper(var.human_user)

  # rsa_public_key must be the PEM body on one line, without the BEGIN/END lines.
  service_public_key = join("", [
    for line in split("\n", trimspace(file(var.service_public_key_path))) :
    trimspace(line) if !startswith(line, "-----")
  ])
}

# The pipeline's key-pair user (Airflow + dbt). Its private key stays in secrets/.
resource "snowflake_service_user" "hdp_service" {
  provider                       = snowflake.securityadmin
  name                           = "HDP_SERVICE"
  default_warehouse              = snowflake_warehouse.hdp.name
  default_role                   = "LOADER"
  default_secondary_roles_option = "NONE"
  rsa_public_key                 = local.service_public_key

  lifecycle {
    # Defaults bootstrap.sql never set; the adopted user reports them, and resetting them is noise.
    ignore_changes = [disabled, display_name, login_name, mins_to_unlock]
  }
}

resource "snowflake_grant_account_role" "service" {
  for_each  = toset(local.service_roles)
  provider  = snowflake.securityadmin
  role_name = snowflake_account_role.this[each.key].name
  user_name = snowflake_service_user.hdp_service.name
}

# Only the grants: the human user itself is never managed here, so Terraform can't lock you out.
resource "snowflake_grant_account_role" "human" {
  for_each  = toset(local.human_roles)
  provider  = snowflake.securityadmin
  role_name = snowflake_account_role.this[each.key].name
  user_name = local.human_user_name
}
