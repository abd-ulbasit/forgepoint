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
  description = "ARN of the Secrets Manager secret holding the DB CONNECTION INFO (host, port, dbname, username, master_user_secret_arn). It contains no password. An ARN is not a secret."
  value       = aws_secretsmanager_secret.db.arn
}

output "credentials_secret_name" {
  description = "Name of the connection-info secret (for External Secrets Operator ExternalSecret refs)."
  value       = aws_secretsmanager_secret.db.name
}

output "kms_key_arn" {
  description = "ARN of the KMS key encrypting RDS storage, the RDS-managed master password secret and the connection-info secret."
  value       = aws_kms_key.rds.arn
}

output "master_user_secret_arn" {
  description = "ARN of the Secrets Manager secret RDS creates and rotates for the master user (JSON: username, password). Grant External Secrets Operator read on THIS arn plus kms:Decrypt on kms_key_arn; no application pod should read it directly."
  value       = aws_db_instance.this.master_user_secret[0].secret_arn
}
