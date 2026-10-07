locals {
  # LOADER: Airflow ingest, writes RAW only. TRANSFORMER: dbt, reads RAW and builds the modeled
  # schemas. ANALYST: reads MARTS with PII masked. PHI_READER: reads MARTS unmasked.
  # PLATFORM_ADMIN: owns the masking policies (snowflake/policies.sql).
  # tests/test_terraform.py checks this list covers every role policies.sql and secure_model use.
  roles = ["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER", "PLATFORM_ADMIN"]
}

resource "snowflake_account_role" "this" {
  for_each = toset(local.roles)
  provider = snowflake.securityadmin
  name     = each.key
}

# Roll every custom role up to SYSADMIN (Snowflake's recommended role hierarchy).
resource "snowflake_grant_account_role" "to_sysadmin" {
  for_each         = toset(local.roles)
  provider         = snowflake.securityadmin
  role_name        = snowflake_account_role.this[each.key].name
  parent_role_name = "SYSADMIN"
}
