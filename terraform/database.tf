locals {
  schemas = ["RAW", "STAGING", "INTERMEDIATE", "MARTS", "SECURITY"]

  # Quoted fully qualified names, built from resource names (known at plan time, and they keep
  # the dependency on the schema). Grants and imports use these.
  schema_fqn = {
    for s in local.schemas :
    s => "\"${snowflake_database.healthcare.name}\".\"${snowflake_schema.this[s].name}\""
  }
}

resource "snowflake_database" "healthcare" {
  provider = snowflake.sysadmin
  name     = "HEALTHCARE"
}

resource "snowflake_schema" "this" {
  for_each = toset(local.schemas)
  provider = snowflake.sysadmin
  database = snowflake_database.healthcare.name
  name     = each.key

  lifecycle {
    # An imported schema reports these as "false" where config says "default"; for is_transient
    # that diff would even force a replacement (drop and recreate, losing data).
    ignore_changes = [is_transient, with_managed_access]
  }
}
