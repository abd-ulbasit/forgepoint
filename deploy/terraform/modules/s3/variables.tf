# ============================================================================
# s3 — INPUTS  (model-artifacts bucket)
# ============================================================================
# The cloud replacement for the local MinIO `model-artifacts` bucket. The Model
# Registry stores ONNX weights here; Model Serving pulls them on startup. Same S3
# API as MinIO, so the app's storage adapter is unchanged — only the endpoint and
# credentials (now IRSA, not static MinIO keys) differ.
# ============================================================================

variable "name_prefix" {
  description = "Name prefix, e.g. \"forgepoint-dev\"."
  type        = string
}

variable "bucket_name" {
  description = "Globally-unique bucket name. S3 bucket names share one global namespace, so we suffix with the account id in the env layer (e.g. forgepoint-dev-model-artifacts-<account>). Passed in so the name is explicit."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for SSE-KMS. Passed in (created in the env layer or eks/rds module) so all artifact encryption can share one key, or set per-env. If empty, the bucket falls back to SSE-S3 (still encrypted, AWS-managed key)."
  type        = string
  default     = ""
}

variable "noncurrent_version_expiration_days" {
  description = "Delete NONCURRENT (old) object versions after this many days. Versioning keeps every overwrite forever otherwise; this caps storage cost for superseded model weights while keeping a recovery window. 90 days dev/prod."
  type        = number
  default     = 90
}

variable "abort_incomplete_multipart_days" {
  description = "Abort + clean up incomplete multipart uploads after N days. Large model artifacts use multipart upload; a failed upload leaves orphaned parts that cost money silently. 7 days reclaims them."
  type        = number
  default     = 7
}

variable "tags" {
  description = "Additional tags (Project/Environment/ManagedBy via provider default_tags)."
  type        = map(string)
  default     = {}
}
