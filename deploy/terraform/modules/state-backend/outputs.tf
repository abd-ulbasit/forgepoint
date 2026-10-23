# ============================================================================
# state-backend — OUTPUTS
# ============================================================================
# These feed straight into environments/<env>/backend.tf after bootstrap:
#   backend "s3" { bucket = <state_bucket_name>, kms_key_id = <kms_key_arn>,
#                  use_lockfile = true, encrypt = true, ... }
# ============================================================================

output "state_bucket_name" {
  description = "Name of the S3 bucket holding Terraform remote state. Paste into each environment's backend config `bucket`."
  value       = aws_s3_bucket.state.id
}

output "state_bucket_arn" {
  description = "ARN of the state bucket (for least-privilege CI roles that need s3:GetObject/PutObject/DeleteObject on just this bucket; DeleteObject is needed to release the .tflock lock file)."
  value       = aws_s3_bucket.state.arn
}

output "kms_key_arn" {
  description = "ARN of the KMS key encrypting state. Paste into each environment's backend config `kms_key_id`; roles that run Terraform need kms:Decrypt and kms:GenerateDataKey on it."
  value       = aws_kms_key.state.arn
}
