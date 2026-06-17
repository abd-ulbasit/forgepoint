# ============================================================================
# iam — INPUTS  (IRSA roles per workload)
# ============================================================================
# IRSA = IAM Roles for Service Accounts. Each role's TRUST policy federates with
# the cluster's OIDC provider and is locked to ONE Kubernetes ServiceAccount
# (namespace:name) via an OIDC `sub` condition. So only a pod running as exactly
# that SA can assume the role — no static AWS keys in any pod, ever.
#
# This module is data-driven: you pass a `roles` map describing each workload's
# SA binding + the AWS access it needs, and the module renders the trust policy +
# permission policies. That keeps the env layer declarative and the module reusable.
# ============================================================================

variable "name_prefix" {
  description = "Name prefix, e.g. \"forgepoint-dev\". Each role is named \"<name_prefix>-irsa-<key>\"."
  type        = string
}

variable "oidc_provider_arn" {
  description = "ARN of the cluster's IAM OIDC provider (from the eks module). The Federated principal in every trust policy."
  type        = string
}

variable "oidc_provider_url" {
  description = "OIDC issuer host WITHOUT https:// (from the eks module). Used to build the `<issuer>:sub` and `:aud` condition keys that pin the role to a specific ServiceAccount."
  type        = string
}

# ----------------------------------------------------------------------------
# The roles to create. A map so each role has a STABLE address (role["registry"])
# regardless of ordering. Each entry binds a K8s ServiceAccount to a least-priv
# set of AWS permissions, expressed as high-level toggles the module turns into
# concrete policy statements (so the env layer never writes raw IAM JSON).
# ----------------------------------------------------------------------------
variable "roles" {
  description = <<-EOT
    Map of IRSA role specs. Key = logical role name. Each value:
      namespace            (string)  K8s namespace of the ServiceAccount (e.g. "fp-system")
      service_account_name (string)  K8s ServiceAccount name the role is bound to
      s3_artifact_access   (string)  ""|"read"|"readwrite" — access to the model-artifacts bucket
      secret_arns          (list)    Secrets Manager ARNs this workload may GetSecretValue on
      kms_key_arns         (list)    KMS key ARNs this workload may Decrypt with (for the above secrets/S3)
    Anything omitted defaults to no access (least privilege).
  EOT
  type = map(object({
    namespace            = string
    service_account_name = string
    s3_artifact_access   = optional(string, "")
    secret_arns          = optional(list(string), [])
    kms_key_arns         = optional(list(string), [])
  }))
  default = {}
}

variable "artifacts_bucket_arn" {
  description = "ARN of the model-artifacts S3 bucket (from the s3 module). Roles with s3_artifact_access get scoped permissions on THIS bucket only."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Additional tags (Project/Environment/ManagedBy via provider default_tags)."
  type        = map(string)
  default     = {}
}
