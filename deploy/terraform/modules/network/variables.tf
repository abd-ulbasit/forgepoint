# ============================================================================
# network — INPUTS
# ============================================================================
# A VPC spanning 3 Availability Zones with a public + private subnet in each.
# Public subnets host only the NAT gateway(s) and load balancers; EVERY workload
# (EKS nodes, RDS, ElastiCache) lives in the PRIVATE subnets with no inbound path
# from the internet. This is the "private-by-default" posture the task requires.
# ============================================================================

variable "name_prefix" {
  description = "Prefix for all resource names, e.g. \"forgepoint-dev\". Keeps names unique per environment so dev/staging/prod can coexist in one account if needed."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC. /16 gives ~65k addresses — ample room to carve 6 subnets and grow. Dev default is fine; prod can keep the same or peer into a wider network plan."
  type        = string
  default     = "10.0.0.0/16"
}

variable "az_count" {
  description = "Number of Availability Zones to spread across. 3 is the floor for true HA (survives one full AZ outage with quorum). EKS control plane and RDS multi-AZ both want >= 2; we standardize on 3."
  type        = number
  default     = 3

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3 (we subnet for at most 3 AZs in this module)."
  }
}

variable "single_nat_gateway" {
  description = "If true, route all private subnets through ONE NAT gateway (cheap: one NAT instead of one-per-AZ). Dev default true to save ~$32/mo/NAT. Prod sets false so each AZ has its own NAT and a single AZ failure doesn't kill egress for the others."
  type        = bool
  default     = true
}

variable "enable_flow_logs" {
  description = "Capture VPC Flow Logs to CloudWatch. ON by default — flow logs are the network audit trail (who talked to whom) and are cheap at dev traffic volumes. Prod keeps them on for security forensics."
  type        = bool
  default     = true
}

variable "flow_logs_retention_days" {
  description = "CloudWatch retention for flow logs. 14 days is plenty for dev debugging; prod typically raises to 90+ for compliance."
  type        = number
  default     = 14
}

variable "aws_region" {
  description = "AWS region — used to build the S3/ECR VPC-endpoint service names (com.amazonaws.<region>.s3 etc.). Passed in rather than read from a data source so the module has no hidden provider dependency."
  type        = string
}

variable "tags" {
  description = "Additional tags merged onto resources (Project/Environment/ManagedBy come from provider default_tags)."
  type        = map(string)
  default     = {}
}
