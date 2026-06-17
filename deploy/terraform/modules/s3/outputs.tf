# ============================================================================
# s3 — OUTPUTS
# ============================================================================
output "bucket_name" {
  description = "Name of the model-artifacts bucket. Apps set FP_S3_BUCKET / the registry config to this."
  value       = aws_s3_bucket.artifacts.id
}

output "bucket_arn" {
  description = "ARN of the bucket. The iam module grants IRSA roles s3:GetObject/PutObject on THIS arn (and arn/*) — least privilege, scoped to just the artifacts bucket."
  value       = aws_s3_bucket.artifacts.arn
}

output "bucket_regional_domain_name" {
  description = "Regional domain name of the bucket (e.g. <name>.s3.<region>.amazonaws.com). Useful for S3 client endpoint config."
  value       = aws_s3_bucket.artifacts.bucket_regional_domain_name
}
