variable "name_prefix" {
  description = "Name prefix, e.g. \"forgepoint-dev\". The role is \"<name_prefix>-github-ecr-push\"."
  type        = string
}

variable "create_oidc_provider" {
  description = "Create the account's GitHub OIDC provider. An account can have only one provider per URL, so set false and pass oidc_provider_arn if it already exists."
  type        = bool
  default     = true
}

variable "oidc_provider_arn" {
  description = "ARN of an existing token.actions.githubusercontent.com provider, used when create_oidc_provider = false."
  type        = string
  default     = ""
}

variable "allowed_subjects" {
  description = "OIDC `sub` patterns allowed to assume the role (StringLike). Keep these to refs that only maintainers can push: the default branch and release tags. Never a pull_request subject or a bare repo:OWNER/REPO:*."
  type        = list(string)

  validation {
    condition     = length(var.allowed_subjects) > 0 && alltrue([for s in var.allowed_subjects : !endswith(s, ":*") && !strcontains(s, "pull_request")])
    error_message = "allowed_subjects must be non-empty and must not allow pull_request runs or a bare ':*' wildcard."
  }
}

variable "ecr_repository_arns" {
  description = "ECR repository ARNs the role may push to (from the ecr module's repository_arns)."
  type        = list(string)
}

variable "tags" {
  description = "Additional tags."
  type        = map(string)
  default     = {}
}
