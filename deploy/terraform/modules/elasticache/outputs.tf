# ============================================================================
# elasticache — OUTPUTS
# ============================================================================
# As with rds: expose endpoint + secret ARN + SG, NEVER the auth token or the
# full rediss:// URL in plaintext outputs.
# ============================================================================

output "primary_endpoint" {
  description = "Primary endpoint hostname for writes. Apps read the full rediss:// URL from Secrets Manager instead."
  value       = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "reader_endpoint" {
  description = "Reader endpoint (load-balances across replicas). Empty/echoes primary on a single-node dev group."
  value       = aws_elasticache_replication_group.this.reader_endpoint_address
}

output "port" {
  description = "Redis port (6379)."
  value       = aws_elasticache_replication_group.this.port
}

output "security_group_id" {
  description = "The Redis security group ID."
  value       = aws_security_group.redis.id
}

output "credentials_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the Redis auth token + URL. The iam module scopes IRSA read access to THIS arn."
  value       = aws_secretsmanager_secret.redis.arn
}

output "credentials_secret_name" {
  description = "Name of the Secrets Manager secret (for External Secrets Operator refs)."
  value       = aws_secretsmanager_secret.redis.name
}

output "kms_key_arn" {
  description = "ARN of the KMS key encrypting Redis at rest + the credentials secret."
  value       = aws_kms_key.redis.arn
}
