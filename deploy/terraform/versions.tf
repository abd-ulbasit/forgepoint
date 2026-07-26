# ============================================================================
# ROOT VERSION PINS — single source of truth for tooling + provider versions
# ============================================================================
# WHY pin here AND in each environment:
#   Terraform does not "inherit" a root versions.tf into child dirs the way a
#   language imports a parent module — each rootmodule (every environments/<env>)
#   resolves its own required_providers. We keep this file as the CANONICAL set
#   of pins and copy the same constraints into environments/*/versions.tf so
#   every env locks identical versions. A drift between them is a review smell.
#
# PINNING PHILOSOPHY:
#   - required_version uses ">= , <" to allow patch/minor bug-fixes but block a
#     surprise MAJOR (e.g. a future 2.x) that could change HCL semantics.
#   - provider versions use "~>" (pessimistic) so we get security/bug patches
#     within a minor line but never an unreviewed minor/major bump. The committed
#     .terraform.lock.hcl then freezes the EXACT version + checksums per platform.
# ============================================================================

terraform {
  # 1.14.x is what this repo is validated against. Allow forward patch/minor,
  # forbid the next major where breaking language changes could land.
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    # The AWS provider is the workhorse — VPC, EKS, RDS, ElastiCache, S3, ECR, IAM.
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    # random: generates DB/Redis passwords we then push to Secrets Manager, so a
    # human never sees or commits them.
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    # tls: used by the eks module to fetch the OIDC provider's CA thumbprint for
    # IRSA (the OIDC trust anchor) without hardcoding a fingerprint.
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    # kubernetes: optionally manage in-cluster objects (IRSA ServiceAccount
    # annotations) from the environment layer after the cluster exists.
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.31"
    }
  }
}
