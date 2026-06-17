# ============================================================================
# network — OUTPUTS
# ============================================================================
# Downstream modules consume these: eks needs subnet IDs + VPC ID; rds and
# elasticache place themselves in the private subnets and restrict SGs to the
# VPC CIDR / EKS node SG.
# ============================================================================

output "vpc_id" {
  description = "ID of the VPC. Needed by every module that creates a security group or subnet group."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "VPC CIDR block. Used by data-store modules to scope security-group ingress to in-VPC traffic only."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnet IDs (NAT + internet-facing load balancers only)."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs — where EKS nodes, RDS, and ElastiCache live. The most-used output in the tree."
  value       = aws_subnet.private[*].id
}

output "availability_zones" {
  description = "AZ names the subnets were placed in (e.g. [us-east-1a, -1b, -1c])."
  value       = local.azs
}

output "nat_gateway_ids" {
  description = "NAT gateway IDs (one or one-per-AZ depending on single_nat_gateway)."
  value       = aws_nat_gateway.this[*].id
}
