output "role_arn" {
  description = "ARN of the push role. Store it as the repository VARIABLE (not secret) AWS_ROLE_ARN and use it as role-to-assume in aws-actions/configure-aws-credentials."
  value       = aws_iam_role.ecr_push.arn
}

output "oidc_provider_arn" {
  description = "ARN of the GitHub OIDC provider the role trusts."
  value       = local.oidc_provider_arn
}
