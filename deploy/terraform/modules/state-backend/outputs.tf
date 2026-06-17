# ============================================================================
# state-backend — OUTPUTS
# ============================================================================
# These feed straight into environments/<env>/backend.tf after bootstrap:
#   backend "s3" { bucket = <this>, dynamodb_table = <this>, ... }
# ============================================================================

output "state_bucket_name" {
  description = "Name of the S3 bucket holding Terraform remote state. Paste into each environment's backend.tf `bucket`."
  value       = aws_s3_bucket.state.id
}

output "state_bucket_arn" {
  description = "ARN of the state bucket (handy for least-privilege CI roles that need s3:GetObject/PutObject on just this bucket)."
  value       = aws_s3_bucket.state.arn
}

output "lock_table_name" {
  description = "Name of the DynamoDB lock table. Paste into each environment's backend.tf `dynamodb_table`."
  value       = aws_dynamodb_table.lock.name
}

output "lock_table_arn" {
  description = "ARN of the DynamoDB lock table (for scoping CI role permissions to just this table)."
  value       = aws_dynamodb_table.lock.arn
}
