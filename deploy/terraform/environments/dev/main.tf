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
  # Dev throwaway: skip the final snapshot so teardown is instant + free.
  skip_final_snapshot = true
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
}

# ----------------------------------------------------------------------------
# (5) S3 — model-artifacts bucket
# ----------------------------------------------------------------------------
# Bucket names are globally unique, so suffix with the account id. We reuse the
# RDS KMS key for SSE-KMS to avoid spinning up yet another CMK in dev; prod could
# pass a dedicated artifacts key.
module "s3" {
  source = "../../modules/s3"

  name_prefix = local.name_prefix
  bucket_name = "${local.name_prefix}-model-artifacts-${data.aws_caller_identity.current.account_id}"
  kms_key_arn = module.rds.kms_key_arn
}

# ----------------------------------------------------------------------------
# (6) ECR — one repo per service (defaults to the full service set)
# ----------------------------------------------------------------------------
module "ecr" {
  source = "../../modules/ecr"

  name_prefix = local.name_prefix
  # Dev: allow destroy to remove repos with images, mutable-tag off (immutable).
  force_delete = true
}

# ----------------------------------------------------------------------------
# (7) IAM — IRSA roles per workload (least privilege)
# ----------------------------------------------------------------------------
# We declare a role for each workload that needs AWS access. The binding is
# (namespace, serviceAccountName) → IAM role. The ServiceAccount names match the
# Helm chart fullnames (e.g. fp-registry) in the fp-system namespace.
#
# Roles modeled here (extend as services gain AWS needs):
#   - registry       : read/write model artifacts in S3 + read its DB secret
#   - model-serving  : READ model artifacts from S3 (pulls weights) + read DB? no.
#                      serving holds models in-memory; it only needs S3 read.
#   - external-secrets: the External Secrets Operator's controller SA — reads the
#                      RDS + Redis secrets from Secrets Manager and projects them
#                      into K8s Secrets for every service. Central, so each app
#                      doesn't need its own Secrets Manager permission.
module "iam" {
  source = "../../modules/iam"

  name_prefix          = local.name_prefix
  oidc_provider_arn    = module.eks.oidc_provider_arn
  oidc_provider_url    = module.eks.oidc_provider_url
  artifacts_bucket_arn = module.s3.bucket_arn

  roles = {
    # Model Registry: writes artifacts on RegisterVersion, reads on serve-prep.
    registry = {
      namespace            = "fp-system"
      service_account_name = "fp-registry"
      s3_artifact_access   = "readwrite"
      secret_arns          = []
      # The artifacts bucket uses the RDS CMK for SSE-KMS (see module "s3"), so
      # reading/writing objects needs kms:Decrypt on that key.
      kms_key_arns = [module.rds.kms_key_arn]
    }

    # Model Serving: pulls (reads) model weights from S3 on startup. Read-only.
    model_serving = {
      namespace            = "fp-models"
      service_account_name = "fp-model-serving"
      s3_artifact_access   = "read"
      secret_arns          = []
      kms_key_arns         = [module.rds.kms_key_arn]
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
        module.elasticache.credentials_secret_arn,
      ]
      kms_key_arns = [
        module.rds.kms_key_arn,
        module.elasticache.kms_key_arn,
      ]
    }
  }
}
