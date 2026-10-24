# ============================================================================
# state-backend — INPUTS
# ============================================================================
# Creates the S3 bucket + KMS key that hold Terraform REMOTE STATE for every
# other stack. This module is the one exception to "state lives in S3": the
# bootstrap root that calls it (deploy/terraform/bootstrap) starts with LOCAL
# state (you can't store state in a bucket that doesn't exist yet). See
# README.md → "Bootstrapping order".
# ============================================================================

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for Terraform state. Convention: forgepoint-tfstate-<account_id>. Passed in (not derived) so the name is explicit and stable. Also used for the KMS alias (alias/<bucket_name>)."
  type        = string
}

variable "state_access_principal_arns" {
  description = "IAM principal ARNs (wildcards allowed, e.g. arn:aws:iam::123456789012:role/terraform-*) that may read, write or delete state objects. When non-empty, the bucket policy DENIES object access to every other principal, so state is readable only by the roles that run Terraform. Empty (the default) adds no such restriction; access is then whatever IAM allows, plus kms:Decrypt on the state key. Bucket-level actions are never denied, so an admin cannot be locked out of the policy itself."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags merged onto every resource. The calling root supplies Project/Environment/ManagedBy via provider default_tags; anything passed here is additive."
  type        = map(string)
  default     = {}
}
