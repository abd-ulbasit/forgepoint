# ============================================================================
# dev — PROVIDER CONFIGURATION
# ============================================================================
# One place to configure the AWS provider (region + the default_tags that satisfy
# the "tag everything" convention) and the Kubernetes provider (so we COULD manage
# in-cluster objects — e.g. ServiceAccount IRSA annotations — from Terraform after
# the cluster exists).
# ----------------------------------------------------------------------------

provider "aws" {
  region = var.aws_region

  # DEFAULT TAGS — applied to EVERY taggable resource created by this provider,
  # automatically. This is how we guarantee Project/Environment/ManagedBy on
  # everything WITHOUT repeating tags in every resource. Modules merge their own
  # Name/Tier tags on top; these three are always present.
  #   ManagedBy=terraform  → signals "do not edit by hand in the console".
  #   Project/Environment  → cost allocation + blast-radius identification.
  default_tags {
    tags = {
      Project     = "forgepoint"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ----------------------------------------------------------------------------
# Kubernetes provider — authenticates to the EKS cluster this stack creates.
# ----------------------------------------------------------------------------
# We pull a short-lived token via the aws_eks_cluster_auth data source rather than
# writing a kubeconfig file — the token is fetched at apply time and never
# persisted. This provider is configured but only used if you add kubernetes_*
# resources (e.g. to stamp IRSA annotations onto ServiceAccounts). The Helm charts
# in deploy/helm remain the primary way workloads are deployed; Terraform owns the
# cloud substrate, Helm/ArgoCD own the apps (clean separation of concerns).
#
# NOTE: a known Terraform sharp edge is configuring the k8s provider from a cluster
# created in the SAME apply (the data source can't read a not-yet-existent
# cluster on the first plan). In practice the cluster is created first; for a
# truly clean split, manage in-cluster resources in a SEPARATE stack that reads
# this one via a remote-state data source. Documented here so the tradeoff is explicit.
data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.this.token
}
