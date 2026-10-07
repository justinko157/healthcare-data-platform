resource "snowflake_warehouse" "hdp" {
  provider            = snowflake.sysadmin
  name                = "HDP_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true

  lifecycle {
    # Only meaningful at creation; an adopted warehouse must not show a diff for it.
    ignore_changes = [initially_suspended]
  }
}
