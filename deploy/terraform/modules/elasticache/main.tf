# ============================================================================
# elasticache — Redis: encrypted (rest+transit), private, SG-restricted, auth token in SM
# ============================================================================
# Security posture mirrors the rds module:
#   - at_rest_encryption_enabled + KMS CMK   → data on disk/snapshots encrypted
#   - transit_encryption_enabled (TLS)       → client↔Redis traffic encrypted
#   - auth_token (generated)                 → password required on every command
#   - SG ingress only from EKS nodes         → no broad network access
#   - auth token stored in Secrets Manager   → never an input/output, never logged
#
# NOTE: transit encryption + auth_token together mean clients MUST connect with
# TLS and AUTH. The app's Redis client config flips to rediss:// + the token —
# the only code change vs the local plaintext Redis.
# ----------------------------------------------------------------------------

locals {
  # ElastiCache replication-group ids must be <= 40 chars and lowercase. Truncate
  # defensively so a long env name doesn't blow the limit.
  replication_group_id = substr("${var.name_prefix}-redis", 0, 40)
}

# ----------------------------------------------------------------------------
# KMS key for at-rest encryption (data + snapshots).
# ----------------------------------------------------------------------------
resource "aws_kms_key" "redis" {
  description             = "${var.name_prefix} ElastiCache Redis at-rest encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  tags                    = merge(var.tags, { Name = "${var.name_prefix}-redis-kms" })
}

resource "aws_kms_alias" "redis" {
  name          = "alias/${var.name_prefix}-redis"
  target_key_id = aws_kms_key.redis.key_id
}

# ----------------------------------------------------------------------------
# Subnet group — pins the cache to PRIVATE subnets.
# ----------------------------------------------------------------------------
resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name_prefix}-redis"
  subnet_ids = var.private_subnet_ids
  tags       = merge(var.tags, { Name = "${var.name_prefix}-redis-subnet-group" })
}

# ----------------------------------------------------------------------------
# Security group — 6379 from EKS nodes only.
# ----------------------------------------------------------------------------
resource "aws_security_group" "redis" {
  name_prefix = "${var.name_prefix}-redis-"
  description = "Redis 6379 from EKS nodes only"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name_prefix}-redis-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "redis_ingress" {
  count                    = length(var.allowed_security_group_ids)
  type                     = "ingress"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = aws_security_group.redis.id
  source_security_group_id = var.allowed_security_group_ids[count.index]
  description              = "Redis from allowed (EKS node) security group"
}

# ----------------------------------------------------------------------------
# Generated AUTH token.
# ----------------------------------------------------------------------------
# ElastiCache AUTH tokens have constraints: 16-128 chars, and only a limited
# special-char set. We restrict override_special accordingly so the token is
# always accepted by ElastiCache.
resource "random_password" "auth_token" {
  length           = 48
  special          = true
  override_special = "!&#$^<>-"
}

# ----------------------------------------------------------------------------
# The replication group.
# ----------------------------------------------------------------------------
# We use aws_elasticache_replication_group (not the legacy single-node
# aws_elasticache_cluster) because it's the ONLY resource that supports
# transit/at-rest encryption + auth_token + automatic failover — the security
# features we require. A 1-node replication group is valid for dev.
resource "aws_elasticache_replication_group" "this" {
  replication_group_id = local.replication_group_id
  description          = "Forgepoint Redis (CQRS read model + rate limiter)"

  engine         = "redis"
  engine_version = var.engine_version
  node_type      = var.node_type
  port           = 6379

  # num_cache_clusters = primary + replicas. automatic_failover needs >= 2.
  num_cache_clusters         = var.num_cache_nodes
  automatic_failover_enabled = var.automatic_failover
  # multi_az requires failover; tie it to the same toggle so they never conflict.
  multi_az_enabled = var.automatic_failover

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  # ENCRYPTION — both directions.
  at_rest_encryption_enabled = true
  kms_key_id                 = aws_kms_key.redis.arn
  transit_encryption_enabled = true
  auth_token                 = random_password.auth_token.result

  # Daily snapshot for recoverability; window outside likely traffic.
  snapshot_retention_limit = var.snapshot_retention_days
  snapshot_window          = "03:00-04:00"
  maintenance_window       = "sun:05:00-sun:06:00"

  # Apply param/maintenance changes in the next window, not immediately, to avoid
  # a surprise mid-day failover. Dev could set true; we keep it safe by default.
  apply_immediately = false

  tags = merge(var.tags, { Name = "${var.name_prefix}-redis" })

  lifecycle {
    ignore_changes = [auth_token]
  }
}

# ----------------------------------------------------------------------------
# Secrets Manager — auth token + connection info (TLS endpoint).
# ----------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "redis" {
  name                    = "${var.name_prefix}/elasticache/redis"
  description             = "Forgepoint Redis auth token + connection info"
  kms_key_id              = aws_kms_key.redis.arn
  recovery_window_in_days = 0
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "redis" {
  secret_id = aws_secretsmanager_secret.redis.id
  secret_string = jsonencode({
    host       = aws_elasticache_replication_group.this.primary_endpoint_address
    port       = aws_elasticache_replication_group.this.port
    auth_token = random_password.auth_token.result
    # rediss:// (note the double-s) = Redis over TLS, required because transit
    # encryption is on. The app uses this URL form instead of plain redis://.
    url = "rediss://:${random_password.auth_token.result}@${aws_elasticache_replication_group.this.primary_endpoint_address}:${aws_elasticache_replication_group.this.port}"
  })
}
