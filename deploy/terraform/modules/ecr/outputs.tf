# ============================================================================
# ecr — OUTPUTS
# ============================================================================
output "repository_urls" {
  description = "Map of service name → repository URL (e.g. \"auth\" => \"<acct>.dkr.ecr.<region>.amazonaws.com/forgepoint-dev/auth\"). CI tags+pushes to these; Helm image.repository points here."
  value       = { for name, repo in aws_ecr_repository.this : name => repo.repository_url }
}

output "repository_arns" {
  description = "Map of service name → repository ARN. Used to scope IAM push/pull permissions per repo if needed."
  value       = { for name, repo in aws_ecr_repository.this : name => repo.arn }
}

output "registry_id" {
  description = "The ECR registry (account) ID hosting these repos."
  value       = values(aws_ecr_repository.this)[0].registry_id
}
