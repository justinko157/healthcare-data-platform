# Adopting an account first set up with the old snowflake/bootstrap.sql.
# The IDs are always computed (tests check they cover every managed instance); the import blocks
# only use them when var.adopt_existing_account is true, because imports fail on a fresh account.
locals {
  q = "\""

  # What bootstrap.sql granted on each schema. It equals local.schema_grants except for the two
  # view layers, where it also granted CREATE TABLE. Importing the live privileges makes the first
  # plan show that cut as an in-place update (CREATE TABLE revoked).
  bootstrap_schema_privileges = merge(local.schema_grants, {
    "TRANSFORMER/STAGING"      = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
    "TRANSFORMER/INTERMEDIATE" = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  })

  adoption_ids = {
    warehouse = { this = "${local.q}HDP_WH${local.q}" }
    database  = { this = "${local.q}HEALTHCARE${local.q}" }
    schemas   = { for s in local.schemas : s => "${local.q}HEALTHCARE${local.q}.${local.q}${s}${local.q}" }
    roles     = { for r in local.roles : r => "${local.q}${r}${local.q}" }
    roles_to_sysadmin = {
      for r in local.roles : r => "${local.q}${r}${local.q}|ROLE|${local.q}SYSADMIN${local.q}"
    }
    warehouse_usage = {
      for r in local.warehouse_users :
      r => "${local.q}${r}${local.q}|false|false|USAGE|OnAccountObject|WAREHOUSE|${local.q}HDP_WH${local.q}"
    }
    database_usage = {
      for r in local.roles :
      r => "${local.q}${r}${local.q}|false|false|USAGE|OnAccountObject|DATABASE|${local.q}HEALTHCARE${local.q}"
    }
    schema_grants = {
      for k, privs in local.bootstrap_schema_privileges :
      k => "${local.q}${split("/", k)[0]}${local.q}|false|false|${join(",", privs)}|OnSchema|OnSchema|${local.q}HEALTHCARE${local.q}.${local.q}${split("/", k)[1]}${local.q}"
    }
    raw_all_tables    = { this = "${local.q}TRANSFORMER${local.q}|false|false|SELECT|OnSchemaObject|OnAll|TABLES|InSchema|${local.q}HEALTHCARE${local.q}.${local.q}RAW${local.q}" }
    raw_future_tables = { this = "${local.q}TRANSFORMER${local.q}|false|false|SELECT|OnSchemaObject|OnFuture|TABLES|InSchema|${local.q}HEALTHCARE${local.q}.${local.q}RAW${local.q}" }
    service_user      = { this = "${local.q}HDP_SERVICE${local.q}" }
    service_roles = {
      for r in local.service_roles : r => "${local.q}${r}${local.q}|USER|${local.q}HDP_SERVICE${local.q}"
    }
    human_roles = {
      for r in local.human_roles : r => "${local.q}${r}${local.q}|USER|${local.q}${local.human_user_name}${local.q}"
    }
  }

  # A filtered comprehension (not a ?: on maps) keeps every kind's type identical either way.
  adopt = {
    for kind, ids in local.adoption_ids :
    kind => { for k, id in ids : k => id if var.adopt_existing_account }
  }
}

import {
  for_each = local.adopt.warehouse
  to       = snowflake_warehouse.hdp
  id       = each.value
}

import {
  for_each = local.adopt.database
  to       = snowflake_database.healthcare
  id       = each.value
}

import {
  for_each = local.adopt.schemas
  to       = snowflake_schema.this[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.roles
  to       = snowflake_account_role.this[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.roles_to_sysadmin
  to       = snowflake_grant_account_role.to_sysadmin[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.warehouse_usage
  to       = snowflake_grant_privileges_to_account_role.warehouse_usage[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.database_usage
  to       = snowflake_grant_privileges_to_account_role.database_usage[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.schema_grants
  to       = snowflake_grant_privileges_to_account_role.schema[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.raw_all_tables
  to       = snowflake_grant_privileges_to_account_role.transformer_raw_all_tables
  id       = each.value
}

import {
  for_each = local.adopt.raw_future_tables
  to       = snowflake_grant_privileges_to_account_role.transformer_raw_future_tables
  id       = each.value
}

import {
  for_each = local.adopt.service_user
  to       = snowflake_service_user.hdp_service
  id       = each.value
}

import {
  for_each = local.adopt.service_roles
  to       = snowflake_grant_account_role.service[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.human_roles
  to       = snowflake_grant_account_role.human[each.key]
  id       = each.value
}
