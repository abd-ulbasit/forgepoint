# ============================================================================
# iam — OUTPUTS
# ============================================================================
# The role_arns map is the bridge to Kubernetes: each value goes into the
# corresponding ServiceAccount's annotation
#   eks.amazonaws.com/role-arn: <arn>
# (set in the Helm values per service, or via the env's kubernetes provider).
# ============================================================================

output "role_arns" {
  description = "Map of logical role name → IAM role ARN. Annotate each K8s ServiceAccount with eks.amazonaws.com/role-arn = <this> to activate IRSA for that workload."
  value       = { for k, r in aws_iam_role.irsa : k => r.arn }
}

output "role_names" {
  description = "Map of logical role name → IAM role name (for reference / further attachments)."
  value       = { for k, r in aws_iam_role.irsa : k => r.name }
}

output "service_account_annotations" {
  description = "Convenience: map of logical role name → the exact SA annotation value to set. Lets the env layer/Helm wire IRSA without reconstructing ARNs."
  value = {
    for k, r in aws_iam_role.irsa : k => {
      namespace            = var.roles[k].namespace
      service_account_name = var.roles[k].service_account_name
      annotation           = { "eks.amazonaws.com/role-arn" = r.arn }
    }
  }
}
