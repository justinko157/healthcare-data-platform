output "roles" {
  description = "The pipeline's account roles."
  value       = sort(keys(snowflake_account_role.this))
}

output "warehouse" {
  description = "Warehouse the pipeline and analysts use."
  value       = snowflake_warehouse.hdp.name
}

output "service_user" {
  description = "Key-pair user the pipeline logs in as."
  value       = snowflake_service_user.hdp_service.name
}
