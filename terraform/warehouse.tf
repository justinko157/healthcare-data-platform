resource "snowflake_warehouse" "hdp" {
  provider            = snowflake.sysadmin
  name                = "HDP_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true

  lifecycle {
    # initially_suspended only matters at creation. The rest are account defaults bootstrap.sql
    # never set; an adopted warehouse reports them, and resetting them would be noise.
    ignore_changes = [
      initially_suspended,
      enable_query_acceleration,
      query_acceleration_max_scale_factor,
      generation,
      warehouse_type,
      scaling_policy,
      min_cluster_count,
      max_cluster_count,
    ]
  }
}
