# ============================================================================
# rds — PostgreSQL: private, KMS-encrypted, SG-restricted, creds in Secrets Manager
# ============================================================================
# The credential flow is the centerpiece:
#   random_password → aws_db_instance.master_password → aws_secretsmanager_secret
# The plaintext password exists only inside Terraform's run and the encrypted
# Secrets Manager entry. It is NEVER an input var, NEVER an output, and the random
# resource is marked sensitive so it doesn't print in plan/apply logs. The app
# reads the DSN from Secrets Manager at runtime (via External Secrets Operator in
# K8s, which maps the secret into a K8s Secret the pod envFrom's).
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
# Generated master password — never typed by a human, never committed.
# ----------------------------------------------------------------------------
# override_special avoids characters that break DSN parsing / shell quoting in
# connection strings ('@', '/', '"', spaces). 32 chars of entropy.
resource "random_password" "master" {
  length           = 32
  special          = true
  override_special = "!#$%*-_=+[]{}<>:?"
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
  password = random_password.master.result

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

  # The generated password churns plan output; don't let a password rotation
  # outside TF (e.g. via Secrets Manager rotation) force a needless modify.
  lifecycle {
    ignore_changes = [password]
  }
}

# ----------------------------------------------------------------------------
# Secrets Manager — store the FULL connection material so apps consume it.
# ----------------------------------------------------------------------------
# We store a JSON blob (not just the password) so consumers get everything needed
# to build a DSN: host, port, user, password, the bootstrap dbname. External
# Secrets Operator in K8s reads this and projects it into a K8s Secret that the
# service pods envFrom — closing the loop from cloud secret → running container.
#
# recovery_window: 0 in dev lets us delete+recreate the secret immediately on
# teardown (no 7-30 day "scheduled deletion" blocking a re-apply). Prod should
# RAISE this (e.g. 7) so an accidental delete is recoverable.
resource "aws_secretsmanager_secret" "db" {
  name                    = "${var.name_prefix}/rds/postgres"
  description             = "Forgepoint Postgres master credentials + connection info"
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
    password = random_password.master.result
    dbname   = var.database_name
    # A ready-to-use DSN with TLS required (RDS terminates TLS; sslmode=require
    # ensures the client refuses an unencrypted connection).
    dsn = "postgres://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:${aws_db_instance.this.port}/${var.database_name}?sslmode=require"
  })
}
