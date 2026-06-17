# ============================================================================
# state-backend — INPUTS
# ============================================================================
# Creates the S3 bucket + DynamoDB lock table that hold Terraform REMOTE STATE
# for every other stack. This module is the one exception to "state lives in S3":
# it is bootstrapped with LOCAL state (you can't store state in a bucket that
# doesn't exist yet), then the bucket it creates becomes the home for all other
# stacks. See README.md → "Bootstrapping order".
# ============================================================================

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for Terraform state. Convention: forgepoint-tfstate-<account_id>. Passed in (not derived) so the name is explicit and stable."
  type        = string
}

variable "lock_table_name" {
  description = "DynamoDB table name used for state locking. One table can lock many state keys (each stack uses a different LockID), so a single table serves all environments."
  type        = string
  default     = "forgepoint-tflock"
}

variable "tags" {
  description = "Tags merged onto every resource. The environments layer supplies Project/Environment/ManagedBy via provider default_tags; anything passed here is additive."
  type        = map(string)
  default     = {}
}
