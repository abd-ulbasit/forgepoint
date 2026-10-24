# ============================================================================
# dev — COMPOSITION  (wire the modules into a working environment)
# ============================================================================
# Dependency order (Terraform infers most of this from references, but reading
# top-to-bottom it is): network → eks → {rds, elasticache, s3, ecr} → iam.
#
#   network ── VPC + private subnets + VPC endpoints
#      │
#      ├── eks ──────── cluster + node group + OIDC provider (for IRSA)
#      │     │
#      │     └── cluster_security_group_id ─┐
#      │     └── oidc_provider_arn/url ─────┼──┐
#      ├── rds ─────────── postgres (private, creds→Secrets Manager)  │  │
#      ├── elasticache ─── redis    (private, token→Secrets Manager)  │  │
#      ├── s3 ──────────── model-artifacts bucket                     │  │
#      └── ecr ─────────── one repo per service                       │  │
#                                                                     │  │
#      iam ◄── consumes EKS OIDC + S3 bucket ARN + secret ARNs ───────┘  │
#          └── builds IRSA roles bound to per-service ServiceAccounts ◄───┘
#
#   budget ── monthly cost budget with email alerts (account-wide)
# ----------------------------------------------------------------------------

locals {
  # One prefix used for naming everything, derived from the project + environment.
  name_prefix = "forgepoint-${var.environment}"
}

# Read the current account id (for globally-unique S3 bucket names) WITHOUT
# hardcoding it — satisfies the "no hardcoded account ids" rule.
data "aws_caller_identity" "current" {}

# ----------------------------------------------------------------------------
# (1) NETWORK
# ----------------------------------------------------------------------------
module "network" {
  source = "../../modules/network"

  name_prefix        = local.name_prefix
  aws_region         = var.aws_region
  vpc_cidr           = var.vpc_cidr
  az_count           = var.az_count
  single_nat_gateway = var.single_nat_gateway
  enable_flow_logs   = true
}

# ----------------------------------------------------------------------------
# (2) EKS
# ----------------------------------------------------------------------------
module "eks" {
  source = "../../modules/eks"

  name_prefix        = local.name_prefix
  cluster_version    = var.cluster_version
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  endpoint_public_access = var.eks_public_access
  public_access_cidrs    = var.eks_public_access_cidrs

  node_instance_types = var.node_instance_types
  node_desired_size   = var.node_desired_size
  node_min_size       = var.node_min_size
  node_max_size       = var.node_max_size
  node_capacity_type  = var.node_capacity_type
}

# ----------------------------------------------------------------------------
# (3) RDS — PostgreSQL, reachable only from the EKS nodes' security group
# ----------------------------------------------------------------------------
module "rds" {
  source = "../../modules/rds"

  name_prefix        = local.name_prefix
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids
  # SG-to-SG: ONLY pods on our EKS nodes may open 5432. The cluster security group
  # is attached to every node ENI, so this is the tightest possible scoping.
  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  engine_version        = var.rds_engine_version
  instance_class        = var.rds_instance_class
  multi_az              = var.rds_multi_az
  deletion_protection   = var.rds_deletion_protection
  backup_retention_days = var.rds_backup_retention_days
  # Destructive settings come from variables with SAFE defaults; the dev
  # teardown values live in terraform.tfvars.example. (They used to be
  # hard-coded here, so a copy of this directory for prod inherited them.)
  skip_final_snapshot         = var.rds_skip_final_snapshot
  secret_recovery_window_days = var.secret_recovery_window_days
}

# ----------------------------------------------------------------------------
# (4) ELASTICACHE — Redis, same SG-restricted posture
# ----------------------------------------------------------------------------
module "elasticache" {
  source = "../../modules/elasticache"

  name_prefix                = local.name_prefix
  vpc_id                     = module.network.vpc_id
  private_subnet_ids         = module.network.private_subnet_ids
  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  engine_version     = var.redis_engine_version
  node_type          = var.redis_node_type
  num_cache_nodes    = var.redis_num_nodes
  automatic_failover = var.redis_automatic_failover

  secret_recovery_window_days = var.secret_recovery_window_days
}

# ----------------------------------------------------------------------------
# (5) S3 — model-artifacts bucket
# ----------------------------------------------------------------------------
# Bucket names are globally unique, so suffix with the account id. The module
# creates a dedicated CMK for the bucket. (It used to reuse the RDS key, which
# gave every artifact reader kms:Decrypt on the key that wraps the DB secrets.)
module "s3" {
  source = "../../modules/s3"

  name_prefix = local.name_prefix
  bucket_name = "${local.name_prefix}-model-artifacts-${data.aws_caller_identity.current.account_id}"
}

# ----------------------------------------------------------------------------
# (6) ECR — one repo per service (defaults to the full service set)
# ----------------------------------------------------------------------------
module "ecr" {
  source = "../../modules/ecr"

  name_prefix = local.name_prefix
  # Images stay immutable (module default). force_delete comes from a variable
  # with a safe default; dev opts in via tfvars so destroy can remove repos.
  force_delete = var.ecr_force_delete
}

# ----------------------------------------------------------------------------
# (7) IAM — IRSA roles per workload (least privilege)
# ----------------------------------------------------------------------------
# We declare a role for each workload that needs AWS access. The binding is
# (namespace, serviceAccountName) → IAM role. The ServiceAccount names match the
# Helm chart fullnames (e.g. fp-registry) in the fp-system namespace.
#
# Roles modeled here (extend as services gain AWS needs):
#   - registry       : read/write model artifacts in S3
#   - model-serving  : READ model artifacts from S3 (pulls weights)
#   NOTE: no service code uses S3 yet (no AWS SDK in any go.mod; model-serving
#   fetches file:// and http(s):// URLs). These two roles are the intended
#   shape; they grant nothing until a ServiceAccount is annotated with them.
#   - external-secrets: the External Secrets Operator's controller SA — reads the
#                      RDS + Redis secrets from Secrets Manager and projects them
#                      into K8s Secrets for every service. Central, so each app
#                      doesn't need its own Secrets Manager permission.
module "iam" {
  source = "../../modules/iam"

  name_prefix           = local.name_prefix
  oidc_provider_arn     = module.eks.oidc_provider_arn
  oidc_provider_url     = module.eks.oidc_provider_url
  artifacts_bucket_arn  = module.s3.bucket_arn
  artifacts_kms_key_arn = module.s3.kms_key_arn

  roles = {
    # Model Registry: writes artifacts on RegisterVersion, reads on serve-prep.
    registry = {
      namespace            = "fp-system"
      service_account_name = "fp-registry"
      s3_artifact_access   = "readwrite"
      secret_arns          = []
      # KMS for the artifacts key comes from artifacts_kms_key_arn above; this
      # role needs no other key.
      kms_key_arns = []
    }

    # Model Serving: pulls (reads) model weights from S3 on startup. Read-only.
    model_serving = {
      namespace            = "fp-models"
      service_account_name = "fp-model-serving"
      s3_artifact_access   = "read"
      secret_arns          = []
      kms_key_arns         = []
    }

    # External Secrets Operator controller: the ONE workload allowed to read the
    # RDS + Redis credential secrets, which it syncs into per-service K8s Secrets.
    # Centralizing this means no application pod needs Secrets Manager access at all.
    external_secrets = {
      namespace            = "fp-system"
      service_account_name = "external-secrets"
      s3_artifact_access   = ""
      secret_arns = [
        module.rds.credentials_secret_arn,
        # The RDS-managed master password (username + password JSON).
        module.rds.master_user_secret_arn,
        module.elasticache.credentials_secret_arn,
      ]
      kms_key_arns = [
        module.rds.kms_key_arn,
        module.elasticache.kms_key_arn,
      ]
    }
  }
}

# ----------------------------------------------------------------------------
# (8) BUDGET — a cost guardrail with email alerts
# ----------------------------------------------------------------------------
# This env runs an EKS control plane, a NAT gateway, interface endpoints, RDS
# and ElastiCache around the clock, so a forgotten `terraform apply` costs
# money every hour. Nothing else in this stack would ever tell you.
#
# Scope: the WHOLE ACCOUNT's monthly cost (no cost filter). Filtering on the
# Project tag would read $0 until that tag is activated as a cost allocation
# tag in the Billing console, which is exactly the silent failure a budget
# exists to prevent. If dev and prod share an account, give prod its own
# budget and raise this one, or switch to a tag filter after activating it.
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_monthly_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.budget_alert_emails
    }
  }

  # Forecast alert: fires early in the month when the run rate says you will
  # blow the budget, before the money is actually spent.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_alert_emails
  }
}

# ----------------------------------------------------------------------------
# (9) CI IDENTITY — GitHub Actions → ECR push via OIDC (OPT-IN, off by default)
# ----------------------------------------------------------------------------
# The ECR repos above had no pusher: CI publishes to GHCR with GITHUB_TOKEN and
# there was no AWS identity for it at all. This role is the keyless way to add
# one: only workflow runs on main or a v* tag can assume it (never a PR run),
# and it can push only to this env's ECR repos. Off by default because no
# workflow pushes to ECR yet and an unused push role is surface for nothing.
# Turn it on with enable_github_ecr_push = true, then store the role_arn output
# as the repository variable AWS_ROLE_ARN.
module "github_oidc" {
  source = "../../modules/github-oidc"
  count  = var.enable_github_ecr_push ? 1 : 0

  name_prefix          = local.name_prefix
  create_oidc_provider = var.create_github_oidc_provider
  oidc_provider_arn    = var.github_oidc_provider_arn
  allowed_subjects     = var.github_oidc_subjects
  ecr_repository_arns  = values(module.ecr.repository_arns)
}
