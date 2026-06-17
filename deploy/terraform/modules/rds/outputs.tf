# ============================================================================
# rds — OUTPUTS
# ============================================================================
# CRITICAL: we expose the Secrets Manager ARN, the endpoint, and the SG — but
# NEVER the password or the full DSN as a plaintext output. Anything that would
# leak the credential is intentionally absent; consumers fetch the secret from
# Secrets Manager at runtime instead.
# ============================================================================

output "endpoint" {
  description = "DB endpoint hostname (without port). For reference/debugging; apps should read the full DSN from Secrets Manager."
  value       = aws_db_instance.this.address
}

output "port" {
  description = "DB port (5432)."
  value       = aws_db_instance.this.port
}

output "instance_identifier" {
  description = "RDS instance identifier."
  value       = aws_db_instance.this.identifier
}

output "security_group_id" {
  description = "The RDS security group ID (for reference / additional rule wiring)."
  value       = aws_security_group.rds.id
}

output "credentials_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the DB DSN/credentials. The iam module grants specific IRSA roles secretsmanager:GetSecretValue on THIS arn so only the right pods can read it. NOT sensitive (an ARN is not a secret), but the secret it points to is."
  value       = aws_secretsmanager_secret.db.arn
}

output "credentials_secret_name" {
  description = "Name of the Secrets Manager secret (handy for External Secrets Operator ExternalSecret refs)."
  value       = aws_secretsmanager_secret.db.name
}

output "kms_key_arn" {
  description = "ARN of the KMS key encrypting RDS storage + the credentials secret."
  value       = aws_kms_key.rds.arn
}
