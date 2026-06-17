# ============================================================================
# dev — OUTPUTS
# ============================================================================
# What an operator needs after `apply`: how to reach the cluster, where the data
# stores are, which Secrets Manager entries hold the credentials, and the ECR
# URLs + IRSA role ARNs to wire into Helm. NO secret VALUES are output — only
# ARNs/names (the secret itself is fetched from Secrets Manager at runtime).
# ============================================================================

# ---- EKS -------------------------------------------------------------------
output "cluster_name" {
  description = "EKS cluster name. `aws eks update-kubeconfig --name <this> --region <region>` to get kubectl access."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint."
  value       = module.eks.cluster_endpoint
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN for the cluster (IRSA trust anchor)."
  value       = module.eks.oidc_provider_arn
}

# ---- Network ---------------------------------------------------------------
output "vpc_id" {
  description = "VPC ID."
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (where workloads + data stores live)."
  value       = module.network.private_subnet_ids
}

# ---- Data stores (endpoints + secret ARNs, NEVER secret values) ------------
output "rds_endpoint" {
  description = "PostgreSQL endpoint hostname. Apps read the full DSN from rds_credentials_secret_arn, not from here."
  value       = module.rds.endpoint
}

output "rds_credentials_secret_arn" {
  description = "Secrets Manager ARN holding the Postgres DSN/credentials. External Secrets Operator (under the external_secrets IRSA role) syncs this into K8s."
  value       = module.rds.credentials_secret_arn
}

output "redis_endpoint" {
  description = "Redis primary endpoint. Apps read the rediss:// URL from redis_credentials_secret_arn."
  value       = module.elasticache.primary_endpoint
}

output "redis_credentials_secret_arn" {
  description = "Secrets Manager ARN holding the Redis auth token + rediss:// URL."
  value       = module.elasticache.credentials_secret_arn
}

# ---- S3 / ECR --------------------------------------------------------------
output "model_artifacts_bucket" {
  description = "Name of the model-artifacts S3 bucket. Set the registry/serving S3 bucket config to this."
  value       = module.s3.bucket_name
}

output "ecr_repository_urls" {
  description = "Map service → ECR repository URL. CI pushes here; Helm image.repository points here."
  value       = module.ecr.repository_urls
}

# ---- IRSA ------------------------------------------------------------------
output "irsa_role_arns" {
  description = "Map of logical workload → IAM role ARN. Annotate each K8s ServiceAccount with eks.amazonaws.com/role-arn = <this> to grant the pod its scoped AWS access without static keys."
  value       = module.iam.role_arns
}

output "irsa_service_account_annotations" {
  description = "Per-role SA binding + the exact annotation to set (namespace, SA name, annotation map). Feed straight into Helm serviceAccount.annotations."
  value       = module.iam.service_account_annotations
}
