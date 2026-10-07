variable "organization_name" {
  description = "Snowflake organization name: the part before the dash in the account identifier (e.g. MVIMJXA)."
  type        = string
}

variable "account_name" {
  description = "Snowflake account name: the part after the dash in the account identifier (e.g. PA76665)."
  type        = string
}

variable "terraform_user" {
  description = "Key-pair service user Terraform logs in as (created by bootstrap_terraform_user.sql)."
  type        = string
  default     = "TERRAFORM"
}

variable "terraform_private_key_path" {
  description = "Private key for terraform_user, relative to terraform/."
  type        = string
  default     = "../secrets/terraform_key.p8"
}

variable "service_public_key_path" {
  description = "Public key for HDP_SERVICE (the pipeline's user), relative to terraform/."
  type        = string
  default     = "../secrets/hdp_service_key.pub"
}

variable "human_user" {
  description = "Your own Snowflake user. Terraform grants it ANALYST and PHI_READER but never manages the user itself."
  type        = string
}

variable "adopt_existing_account" {
  description = "true only for an account first set up with the old snowflake/bootstrap.sql: import its objects instead of creating them."
  type        = bool
  default     = false
}
