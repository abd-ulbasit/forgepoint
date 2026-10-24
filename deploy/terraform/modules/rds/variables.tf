# ============================================================================
# rds — INPUTS
# ============================================================================
# A PostgreSQL instance, private-subnet only, encrypted with KMS, reachable ONLY
# from the EKS nodes. The master password is generated, stored and rotated by
# RDS itself (manage_master_user_password): it is never an input variable and
# never passes through Terraform, so it is not in plan output or state.
#
# DATABASE-PER-SERVICE note: locally we run one Postgres server with one database
# per service (fp_auth, fp_registry, …). We mirror that here: one RDS INSTANCE,
# and the per-service databases are created by the existing app bootstrap
# (scripts/init-postgres.sh / migrations) against this instance — same boundary,
# same connection-string-per-service model. Splitting into one RDS instance per
# service is a prod option (toggle by instantiating this module N times), but is
# overkill + costly for dev. This is the documented tradeoff.
# ============================================================================

variable "name_prefix" {
  description = "Name prefix, e.g. \"forgepoint-dev\"."
  type        = string
}

variable "vpc_id" {
  description = "VPC the database lives in."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the DB subnet group. RDS spans these; with multi-AZ the standby lands in a different AZ's subnet automatically. NO public subnets — the DB is never internet-reachable."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security group IDs permitted to connect on 5432. In practice this is the EKS cluster/node SG — so ONLY pods on our nodes can reach Postgres. SG-to-SG rules are tighter than CIDR rules (they follow the instances, not an IP range)."
  type        = list(string)
  default     = []
}

variable "engine_version" {
  description = "PostgreSQL major.minor. Matches the local Postgres 17 image family. Pin it; RDS only auto-applies minor patches if allow_minor_version_upgrade is on."
  type        = string
  default     = "16.4"
}

variable "instance_class" {
  description = "RDS instance class. Dev: db.t3.micro (burstable, free-tier eligible). Prod tunes to db.m6g/db.r6g with provisioned IOPS."
  type        = string
  default     = "db.t3.micro"
}

variable "allocated_storage" {
  description = "Initial storage (GiB). 20 is the gp3 floor and plenty for dev. max_allocated_storage enables autoscaling above this."
  type        = number
  default     = 20
}

variable "max_allocated_storage" {
  description = "Storage autoscaling ceiling (GiB). RDS grows storage automatically up to this when usage climbs, so we don't pre-pay for capacity. 100 in dev."
  type        = number
  default     = 100
}

variable "multi_az" {
  description = "Run a synchronous standby in a second AZ with automatic failover. FALSE in dev (halves cost; a dev DB can tolerate a few minutes of downtime). TRUE in prod — this is the headline availability lever."
  type        = bool
  default     = false
}

variable "database_name" {
  description = "Name of the INITIAL database created with the instance. The per-service databases (fp_auth, …) are created afterwards by the app's init/migrations against this instance. Keeping one bootstrap DB matches the local model."
  type        = string
  default     = "forgepoint"
}

variable "master_username" {
  description = "Master DB username. NOT a secret on its own (the PASSWORD is what's secret); kept as a var for clarity. The password is generated and rotated by RDS in a Secrets Manager secret it manages."
  type        = string
  default     = "fp_admin"
}

variable "backup_retention_days" {
  description = "Automated backup retention (days). 7 in dev gives a week of point-in-time recovery; prod commonly uses 14-35. >0 also ENABLES automated backups + PITR."
  type        = number
  default     = 7
}

variable "deletion_protection" {
  description = "Block `terraform destroy`/console delete of the instance. SAFE DEFAULT true. A throwaway dev env sets false in its tfvars so it can be torn down."
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Skip the final snapshot on delete. SAFE DEFAULT false, so a delete always leaves a recoverable snapshot. A throwaway dev env sets true in its tfvars."
  type        = bool
  default     = false
}

variable "secret_recovery_window_days" {
  description = "Days Secrets Manager keeps the deleted connection-info secret recoverable (7-30), or 0 to delete immediately. SAFE DEFAULT 30 (the AWS default). A throwaway dev env may set 0 so destroy + re-apply does not collide with a secret pending deletion."
  type        = number
  default     = 30

  validation {
    condition     = var.secret_recovery_window_days == 0 || (var.secret_recovery_window_days >= 7 && var.secret_recovery_window_days <= 30)
    error_message = "secret_recovery_window_days must be 0 or between 7 and 30."
  }
}

variable "tags" {
  description = "Additional tags (Project/Environment/ManagedBy via provider default_tags)."
  type        = map(string)
  default     = {}
}
