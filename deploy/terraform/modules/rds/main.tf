# ============================================================================
# rds — PostgreSQL: private, KMS-encrypted, SG-restricted, creds in Secrets Manager
# ============================================================================
# The credential flow is the centerpiece:
#   manage_master_user_password = true
#     → RDS generates the master password itself, stores it in a Secrets Manager
#       secret it owns (rds!db-...), encrypted with our CMK, and rotates it.
# Terraform never sees the password, so it is not in state, not in plan output
# and not in any secret_string Terraform writes. (The previous design generated
# it with random_password and wrote a full DSN into a secret version, which put
# the plaintext into state, and the state bucket was only SSE-S3.)
#
# The app still gets a ready DSN: External Secrets Operator reads username +
# password from the RDS-managed secret and host/port/dbname from the
# connection-info secret below, and renders FP_DATABASE_URL with a template
# (deploy/secrets/external-secrets/). Rotation: RDS rotates the managed secret
# (every 7 days by default); ESO picks the new value up on its refresh, but env
# vars only change when a pod restarts, so pair it with a reloader or a
# scheduled rollout. Existing connections survive a rotation; new ones need
# the new password.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# KMS key — encrypts the DB storage at rest (and automated backups/snapshots).
# ----------------------------------------------------------------------------
resource "aws_kms_key" "rds" {
  description             = "${var.name_prefix} RDS storage encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  tags                    = merge(var.tags, { Name = "${var.name_prefix}-rds-kms" })
}

resource "aws_kms_alias" "rds" {
  name          = "alias/${var.name_prefix}-rds"
  target_key_id = aws_kms_key.rds.key_id
}

# ----------------------------------------------------------------------------
# DB subnet group — pins RDS to the PRIVATE subnets across AZs.
# ----------------------------------------------------------------------------
resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-db"
  subnet_ids = var.private_subnet_ids
  tags       = merge(var.tags, { Name = "${var.name_prefix}-db-subnet-group" })
}

# ----------------------------------------------------------------------------
# Security group — ingress 5432 ONLY from the allowed SGs (EKS nodes).
# ----------------------------------------------------------------------------
# No CIDR ingress at all: access is granted SG-to-SG, so only instances in the
# EKS node SG can open a connection. There is no public path and no broad subnet
# rule. Egress is left open (the DB initiates nothing meaningful outbound).
resource "aws_security_group" "rds" {
  name_prefix = "${var.name_prefix}-rds-"
  description = "Postgres 5432 from EKS nodes only"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name_prefix}-rds-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

# One ingress rule per allowed SG. Using the dedicated rule resource (not inline)
# lets the set of allowed SGs change without recreating the SG itself.
resource "aws_security_group_rule" "rds_ingress" {
  count                    = length(var.allowed_security_group_ids)
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = var.allowed_security_group_ids[count.index]
  description              = "PostgreSQL from allowed (EKS node) security group"
}

# ----------------------------------------------------------------------------
# The instance.
# ----------------------------------------------------------------------------
resource "aws_db_instance" "this" {
  identifier     = "${var.name_prefix}-postgres"
  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  # gp3 storage, encrypted with our CMK. storage autoscaling up to the ceiling.
  storage_type          = "gp3"
  allocated_storage     = var.allocated_storage
  max_allocated_storage = var.max_allocated_storage
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.rds.arn

  db_name  = var.database_name
  username = var.master_username
  # RDS owns the password: generated, stored in a Secrets Manager secret it
  # manages (encrypted with our CMK, not the AWS-managed key) and rotated. It
  # never passes through Terraform, so it never lands in state.
  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.rds.arn

  # PRIVATE: in our subnet group, attached to the restricted SG, and explicitly
  # NOT publicly accessible — three independent guarantees of no internet exposure.
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  multi_az = var.multi_az

  # Backups + point-in-time recovery. A backup window outside likely traffic.
  backup_retention_period = var.backup_retention_days
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:30-sun:05:30"
  copy_tags_to_snapshot   = true

  # Safety rails (prod tightens both via tfvars).
  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : "${var.name_prefix}-postgres-final"

  # Minor patches only within the pinned major; never silent major upgrades.
  auto_minor_version_upgrade = true

  # Surface engine logs in CloudWatch for debugging slow queries / connection errors.
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = merge(var.tags, { Name = "${var.name_prefix}-postgres" })

}

# ----------------------------------------------------------------------------
# Secrets Manager — connection INFO only (no password).
# ----------------------------------------------------------------------------
# host, port, dbname, username and the ARN of the RDS-managed password secret.
# None of it is a credential; it lives in Secrets Manager (rather than only in
# outputs) so External Secrets Operator can read it next to the password and
# render the DSN in-cluster. Keeping the password OUT of this secret is the
# point: the old design wrote the plaintext into secret_string, i.e. into state,
# and its aws_db_instance ignored password changes while this resource did not,
# so an out-of-band rotation set up a later apply that would overwrite the
# secret with a password the database no longer accepted.
#
# recovery_window: 0 in dev lets us delete+recreate the secret immediately on
# teardown (no 7-30 day "scheduled deletion" blocking a re-apply). Prod should
# RAISE this (e.g. 7) so an accidental delete is recoverable.
resource "aws_secretsmanager_secret" "db" {
  name                    = "${var.name_prefix}/rds/postgres"
  description             = "Forgepoint Postgres connection info (no password; see master_user_secret_arn)"
  kms_key_id              = aws_kms_key.rds.arn
  recovery_window_in_days = 0
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.this.address
    port     = aws_db_instance.this.port
    username = var.master_username
    dbname   = var.database_name
    # Clients must use TLS (sslmode=require); ESO's DSN template adds it.
    sslmode = "require"
    # Where the password lives (RDS-managed, rotated). ESO reads it from there.
    master_user_secret_arn = aws_db_instance.this.master_user_secret[0].secret_arn
  })
}
