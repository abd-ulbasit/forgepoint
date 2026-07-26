# ============================================================================
# network — VPC, subnets (public + PRIVATE x3 AZ), IGW, NAT, VPC endpoints, flow logs
# ============================================================================
#
#                          ┌──────────────── VPC 10.0.0.0/16 ────────────────┐
#   internet ── IGW ───────┤  public-a   public-b   public-c   (NAT + LB only)│
#                          │     │  (NAT gw lives here)                       │
#                          │     ▼ default route 0.0.0.0/0 → NAT              │
#                          │  private-a  private-b  private-c                 │
#                          │   (EKS nodes, RDS, ElastiCache — NO inbound)     │
#                          │   S3/ECR traffic ── VPC endpoints ──▶ AWS (no NAT)│
#                          └──────────────────────────────────────────────────┘
#
# DESIGN CHOICES:
#   * public vs private split: only NAT + load balancers sit public. Databases and
#     pods are private — they reach OUT via NAT but nothing reaches IN. Classic
#     three-tier isolation.
#   * VPC ENDPOINTS for S3 (gateway) and ECR (interface): pulling model artifacts
#     from S3 and container images from ECR would otherwise traverse the NAT gw and
#     incur per-GB NAT data-processing charges. Routing that traffic through VPC
#     endpoints keeps it on the AWS backbone — cheaper AND more secure (never
#     leaves the VPC). This directly addresses the task's "reduce NAT cost" ask.
# ----------------------------------------------------------------------------

# Discover the AZ names available in this region. Using a data source (instead of
# hardcoding "us-east-1a") makes the module region-portable.
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # Take the first N AZs the region offers.
  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  # Carve the /16 into /20 subnets deterministically with cidrsubnet():
  #   public  subnets get the FIRST az_count /20 blocks
  #   private subnets get the NEXT az_count /20 blocks (offset by az_count)
  # /20 = 4096 IPs/subnet — private subnets need room for many pod IPs (the EKS
  # VPC-CNI assigns a real VPC IP per pod), so we keep them generous.
  public_subnet_cidrs  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  private_subnet_cidrs = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i + var.az_count)]

  # How many NAT gateways to actually build (1 shared vs one-per-AZ).
  nat_gateway_count = var.single_nat_gateway ? 1 : var.az_count
}

# ----------------------------------------------------------------------------
# VPC
# ----------------------------------------------------------------------------
# enable_dns_hostnames + enable_dns_support are REQUIRED for:
#   - private DNS on the interface VPC endpoints (ECR/STS) to resolve, and
#   - EKS worker nodes to register by hostname.
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, { Name = "${var.name_prefix}-vpc" })
}

# Internet Gateway — the VPC's door to the internet. Public subnets route their
# default route here; private subnets never do.
resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name_prefix}-igw" })
}

# ----------------------------------------------------------------------------
# Subnets
# ----------------------------------------------------------------------------
# PUBLIC subnets. map_public_ip_on_launch = true so a NAT gateway / LB placed
# here gets a public IP. The kubernetes.io/role/elb tag tells the AWS Load
# Balancer Controller "put internet-facing LBs in these subnets".
resource "aws_subnet" "public" {
  count                   = var.az_count
  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_subnet_cidrs[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true

  tags = merge(var.tags, {
    Name                     = "${var.name_prefix}-public-${local.azs[count.index]}"
    Tier                     = "public"
    "kubernetes.io/role/elb" = "1"
  })
}

# PRIVATE subnets — where the real workloads live. No auto public IP. The
# kubernetes.io/role/internal-elb tag steers INTERNAL load balancers here.
resource "aws_subnet" "private" {
  count             = var.az_count
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_subnet_cidrs[count.index]
  availability_zone = local.azs[count.index]

  tags = merge(var.tags, {
    Name                              = "${var.name_prefix}-private-${local.azs[count.index]}"
    Tier                              = "private"
    "kubernetes.io/role/internal-elb" = "1"
  })
}

# ----------------------------------------------------------------------------
# NAT gateways — let private subnets reach OUT (pull images, OS updates, call AWS
# APIs that lack an endpoint) while staying unreachable from the internet.
# ----------------------------------------------------------------------------
# Each NAT needs a stable public IP (EIP). domain="vpc" is the modern arg.
resource "aws_eip" "nat" {
  count  = local.nat_gateway_count
  domain = "vpc"
  tags   = merge(var.tags, { Name = "${var.name_prefix}-nat-eip-${count.index}" })

  # NAT EIPs depend on the IGW being attached (AWS requirement for VPC EIPs).
  depends_on = [aws_internet_gateway.this]
}

resource "aws_nat_gateway" "this" {
  count         = local.nat_gateway_count
  allocation_id = aws_eip.nat[count.index].id
  # NAT gateways live in PUBLIC subnets (they need an internet-facing path).
  subnet_id  = aws_subnet.public[count.index].id
  tags       = merge(var.tags, { Name = "${var.name_prefix}-nat-${count.index}" })
  depends_on = [aws_internet_gateway.this]
}

# ----------------------------------------------------------------------------
# Route tables
# ----------------------------------------------------------------------------
# ONE public route table shared by all public subnets: default route → IGW.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = merge(var.tags, { Name = "${var.name_prefix}-public-rt" })
}

resource "aws_route_table_association" "public" {
  count          = var.az_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# PRIVATE route tables: ONE PER AZ. Why per-AZ and not shared? When
# single_nat_gateway=false each private subnet must route to the NAT IN ITS OWN AZ
# (so an AZ outage doesn't force cross-AZ egress or break it entirely). Per-AZ
# route tables are the clean way to express that. With a single NAT they all point
# at the same NAT — same structure, simpler to flip the toggle.
resource "aws_route_table" "private" {
  count  = var.az_count
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    # If single NAT: every table → nat[0]. If per-AZ: table i → nat[i].
    nat_gateway_id = var.single_nat_gateway ? aws_nat_gateway.this[0].id : aws_nat_gateway.this[count.index].id
  }
  tags = merge(var.tags, { Name = "${var.name_prefix}-private-rt-${local.azs[count.index]}" })
}

resource "aws_route_table_association" "private" {
  count          = var.az_count
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# ----------------------------------------------------------------------------
# VPC ENDPOINTS — keep S3 + ECR traffic off the NAT (cost) and inside AWS (security)
# ----------------------------------------------------------------------------
# S3 GATEWAY endpoint: free, route-table based. Adds a route so traffic to S3's
# prefix list goes direct instead of via NAT. Model-artifact pulls (potentially
# many GB) thus incur ZERO NAT data-processing charge. This is the single biggest
# NAT-cost lever for an ML platform that streams model weights from S3.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  # Attach to every private route table so private workloads use it.
  route_table_ids = aws_route_table.private[*].id
  tags            = merge(var.tags, { Name = "${var.name_prefix}-s3-endpoint" })
}

# ECR needs THREE interface endpoints to pull an image without NAT:
#   ecr.api  — auth/manifest calls   ecr.dkr — the docker registry data path
#   ...and the layers themselves come from S3, covered by the gateway endpoint above.
# Interface endpoints are ENIs in the private subnets, fronted by a tiny SG that
# allows 443 from within the VPC. private_dns_enabled=true means the normal ECR
# hostnames resolve to these ENIs transparently — no app config change.
locals {
  interface_endpoints = {
    ecr_api = "com.amazonaws.${var.aws_region}.ecr.api"
    ecr_dkr = "com.amazonaws.${var.aws_region}.ecr.dkr"
  }
}

# Dedicated SG for the interface endpoints: ingress 443 from the VPC CIDR only.
resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${var.name_prefix}-vpce-"
  description = "Allow HTTPS from within the VPC to interface VPC endpoints (ECR)"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "HTTPS from within the VPC to the endpoint ENIs"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Endpoints don't initiate outbound connections, but a permissive egress keeps
  # the ENI from blackholing return traffic edge cases; scoped to the VPC.
  egress {
    description = "Return traffic within the VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-vpce-sg" })

  # Avoid a delete-before-create clash when the SG name must change.
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_endpoint" "interface" {
  for_each            = local.interface_endpoints
  vpc_id              = aws_vpc.this.id
  service_name        = each.value
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = merge(var.tags, { Name = "${var.name_prefix}-${each.key}-endpoint" })
}

# ----------------------------------------------------------------------------
# VPC FLOW LOGS — the network audit trail
# ----------------------------------------------------------------------------
# Records ACCEPT/REJECT for traffic across the VPC's ENIs. Indispensable for
# security forensics ("did anything talk to RDS it shouldn't have?") and for
# debugging NetworkPolicy/SG drops. Sent to CloudWatch Logs.
resource "aws_cloudwatch_log_group" "flow_logs" {
  count             = var.enable_flow_logs ? 1 : 0
  name              = "/forgepoint/${var.name_prefix}/vpc-flow-logs"
  retention_in_days = var.flow_logs_retention_days
  tags              = var.tags
}

# IAM role the VPC Flow Logs service assumes to write into CloudWatch.
data "aws_iam_policy_document" "flow_logs_assume" {
  count = var.enable_flow_logs ? 1 : 0
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flow_logs" {
  count              = var.enable_flow_logs ? 1 : 0
  name_prefix        = "${var.name_prefix}-flowlogs-"
  assume_role_policy = data.aws_iam_policy_document.flow_logs_assume[0].json
  tags               = var.tags
}

# Least-privilege: the role may only write to OUR flow-log group, nothing else.
data "aws_iam_policy_document" "flow_logs_permissions" {
  count = var.enable_flow_logs ? 1 : 0
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["${aws_cloudwatch_log_group.flow_logs[0].arn}:*"]
  }
}

resource "aws_iam_role_policy" "flow_logs" {
  count  = var.enable_flow_logs ? 1 : 0
  name   = "flow-logs-write"
  role   = aws_iam_role.flow_logs[0].id
  policy = data.aws_iam_policy_document.flow_logs_permissions[0].json
}

resource "aws_flow_log" "this" {
  count                = var.enable_flow_logs ? 1 : 0
  vpc_id               = aws_vpc.this.id
  traffic_type         = "ALL" # ACCEPT + REJECT — see drops too, not just allows
  iam_role_arn         = aws_iam_role.flow_logs[0].arn
  log_destination_type = "cloud-watch-logs"
  log_destination      = aws_cloudwatch_log_group.flow_logs[0].arn
  tags                 = merge(var.tags, { Name = "${var.name_prefix}-flow-log" })
}
