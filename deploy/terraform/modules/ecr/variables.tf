# ============================================================================
# ecr — INPUTS  (one repository per service)
# ============================================================================
# Locally we push images to ghcr.io; in AWS the cluster pulls from ECR (via the
# ECR VPC endpoints the network module created, so pulls don't traverse NAT). One
# repo per service keeps IAM scoping and lifecycle policy per-service clean.
# ============================================================================

variable "name_prefix" {
  description = "Name prefix, e.g. \"forgepoint-dev\". Each repo is named \"<name_prefix>/<service>\" so dev/prod images don't collide in one account."
  type        = string
}

variable "service_names" {
  description = "The services to create repositories for. Defaults to the full 10-service set from the design doc plus the BFF. Override to add/remove."
  type        = list(string)
  default = [
    "auth",
    "registry",
    "inference-gateway",
    "pipeline-orchestrator",
    "feature-store",
    "experiment-tracker",
    "billing",
    "notification",
    "model-serving",
    "model-monitor",
    "bff",
  ]
}

variable "image_tag_mutability" {
  description = "IMMUTABLE or MUTABLE tags. IMMUTABLE by default — once :v1.2.3 is pushed it can NEVER be overwritten, which guarantees a deployed tag always means the exact same bytes (reproducibility + supply-chain integrity). The cost: you must push a new tag for every change (no re-pushing :latest)."
  type        = string
  default     = "IMMUTABLE"

  validation {
    condition     = contains(["IMMUTABLE", "MUTABLE"], var.image_tag_mutability)
    error_message = "image_tag_mutability must be IMMUTABLE or MUTABLE."
  }
}

variable "scan_on_push" {
  description = "Run the ECR vulnerability scan automatically on every push. ON by default — catches known CVEs in image layers before they ever run. Complements the Trivy/govulncheck CI gates."
  type        = bool
  default     = true
}

variable "untagged_expiry_days" {
  description = "Expire UNTAGGED images after this many days. Every new push that reuses a digest orphans the old untagged manifest; without this they accumulate and cost money. 14 days reclaims them while leaving a debugging window."
  type        = number
  default     = 14
}

variable "max_tagged_images" {
  description = "Keep at most this many TAGGED images per repo (newest by push). Caps storage for old releases while retaining enough for rollback. 20 is generous for dev."
  type        = number
  default     = 20
}

variable "force_delete" {
  description = "Allow `terraform destroy` to delete a repo that still contains images. TRUE in dev (tear it all down). FALSE in prod so you can't wipe release images by accident."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags (Project/Environment/ManagedBy via provider default_tags)."
  type        = map(string)
  default     = {}
}
