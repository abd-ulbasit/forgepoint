# ============================================================================
# eks — OUTPUTS
# ============================================================================
# The OIDC outputs are the bridge to the iam module: it builds IRSA trust
# policies from oidc_provider_arn + oidc_provider_url. The endpoint/CA/auth
# outputs configure the kubernetes/helm providers in the environment layer.
# ============================================================================

output "cluster_name" {
  description = "EKS cluster name. Used by kubectl/helm and by the aws_eks_cluster_auth data source for provider auth."
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint URL."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64 CA cert for the API server — the kubernetes provider needs this to verify TLS."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_security_group_id" {
  description = "The cluster's MANAGED security group (EKS-created). Data stores (RDS/ElastiCache) can allow ingress from this SG to permit node→DB traffic precisely, rather than by CIDR."
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "oidc_provider_arn" {
  description = "ARN of the IAM OIDC provider. The iam module references this in IRSA role trust policies (the Federated principal)."
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_provider_url" {
  description = "OIDC issuer URL WITHOUT the https:// scheme (e.g. oidc.eks.us-east-1.amazonaws.com/id/ABC). IRSA trust-policy conditions key off this host."
  value       = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
}

output "node_role_arn" {
  description = "IAM role ARN attached to worker nodes (for reference / aws-auth mapping)."
  value       = aws_iam_role.node.arn
}

output "kms_key_arn" {
  description = "ARN of the KMS key encrypting Kubernetes Secrets in etcd."
  value       = aws_kms_key.eks_secrets.arn
}

output "node_ebs_kms_key_arn" {
  description = "ARN of the KMS CMK encrypting worker-node root EBS volumes. Useful if another stack needs to add a kms:Decrypt grant (e.g. a backup solution). Empty string if node_ebs_kms_key_arn was passed in externally (caller already owns that key)."
  value       = var.node_ebs_kms_key_arn != "" ? var.node_ebs_kms_key_arn : aws_kms_key.node_ebs[0].arn
}
