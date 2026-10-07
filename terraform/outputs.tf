output "roles" {
  description = "The pipeline's account roles."
  value       = sort(keys(snowflake_account_role.this))
}

output "warehouse" {
  description = "Warehouse the pipeline and analysts use."
  value       = snowflake_warehouse.hdp.name
}
