# ============================================================================
# eks — INPUTS
# ============================================================================
# An EKS cluster with: a restricted/private API endpoint, a managed node group in
# the private subnets, the OIDC provider enabled (so workloads can use IRSA — IAM
# Roles for Service Accounts), envelope encryption of Kubernetes Secrets via a
# customer-managed KMS key, and control-plane audit logging to CloudWatch.
# ============================================================================

variable "name_prefix" {
  description = "Prefix for names, e.g. \"forgepoint-dev\". The cluster is named \"<name_prefix>\"."
  type        = string
}

variable "cluster_version" {
  description = "Kubernetes minor version for the control plane (e.g. \"1.30\"). Pin it: EKS won't auto-upgrade the control plane, and node AMIs are chosen to match. Bump deliberately, one minor at a time."
  type        = string
  default     = "1.30"
}

variable "vpc_id" {
  description = "VPC to launch the cluster into (from the network module)."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the control-plane ENIs AND the worker nodes. Nodes live ONLY in private subnets — never publicly addressable."
  type        = list(string)
}

variable "endpoint_public_access" {
  description = "Whether the Kubernetes API server is reachable from the public internet. SECURE DEFAULT: false — the API is reachable only from within the VPC (bastion/VPN). Set true in dev tfvars if you need kubectl from a laptop without VPN, AND also set public_access_cidrs to your egress IP. Never leave the public endpoint open to 0.0.0.0/0 in any long-lived environment."
  type        = bool
  default     = false
}

variable "public_access_cidrs" {
  description = "When endpoint_public_access=true, ONLY these CIDRs may reach the API. SECURE DEFAULT: empty list (enforced by EKS to deny all public access when endpoint_public_access=false). When enabling the public endpoint, set this to your VPN/office egress IP (e.g. [\"203.0.113.10/32\"]) — never leave as 0.0.0.0/0. The default intentionally contains no open range so any accidental public-endpoint enable is not also world-open."
  type        = list(string)
  default     = []
}

# --- Managed node group sizing (dev defaults are small/cheap) ---------------
variable "node_instance_types" {
  description = "EC2 instance types for the managed node group. Dev: t3.medium (2 vCPU/4GiB) — enough to run the 10 services + infra add-ons cheaply. Prod tunes up (m5.large+) and may add a second node group."
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_desired_size" {
  description = "Desired node count. 2 in dev for minimal HA across AZs without burning budget."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum nodes the autoscaler/EKS will keep. 2 keeps the platform up during a single-node failure."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Maximum nodes. Caps blast radius and cost. Dev 4; prod raises with load."
  type        = number
  default     = 4
}

variable "node_disk_size" {
  description = "EBS root volume size (GiB) per node. 20 fits distroless service images + a few cached model artifacts. Raise for image-heavy nodes."
  type        = number
  default     = 20
}

variable "node_ebs_kms_key_arn" {
  description = "ARN of the KMS CMK used to encrypt worker-node root EBS volumes. If empty string, the module creates a dedicated node-EBS CMK automatically. Explicitly pass the ARN to share an existing key (e.g. reuse the EKS-secrets CMK). SECURITY NOTE: root volumes hold container-layer overlays and any secrets written to disk by a running pod — encrypting them with a CMK means you can revoke access (cut the key) in an incident and you have CloudTrail coverage of every decrypt/re-encrypt."
  type        = string
  default     = ""
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT. Dev can use SPOT to cut node cost ~70% (services are stateless + replicated, so an interrupted node just reschedules). Default ON_DEMAND for predictability; flip to SPOT in dev tfvars to save money."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.node_capacity_type)
    error_message = "node_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "enabled_cluster_log_types" {
  description = "Control-plane log streams to ship to CloudWatch. We enable the security-relevant ones by default: api, audit, authenticator. 'audit' is the K8s API audit log — the backbone of any cluster-security investigation."
  type        = list(string)
  default     = ["api", "audit", "authenticator"]
}

variable "cluster_log_retention_days" {
  description = "Retention for the control-plane CloudWatch log group. 14 days dev; prod raises for compliance."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Additional tags (Project/Environment/ManagedBy come from provider default_tags)."
  type        = map(string)
  default     = {}
}
