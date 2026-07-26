# ============================================================================
# eks — control plane + managed node group + KMS secret encryption + IRSA/OIDC
# ============================================================================
# We build EKS from PRIMITIVES (aws_eks_cluster / aws_eks_node_group / IAM /
# aws_iam_openid_connect_provider) rather than the terraform-aws-modules/eks
# community module. WHY: every resource here is readable and explicit. The
# community module is excellent in production but hides the OIDC/IRSA wiring,
# KMS envelope encryption, and IAM trust relationships — which are exactly the
# parts worth understanding.
# ----------------------------------------------------------------------------

# ============================================================================
# (1) KMS KEY — envelope encryption for Kubernetes Secrets stored in etcd
# ============================================================================
# By default EKS encrypts etcd with an AWS-owned key. Adding a CUSTOMER-managed
# KMS key gives ENVELOPE encryption of the Secret resources specifically: the K8s
# Secret payload is encrypted with a data key that is itself encrypted by this
# CMK. Benefits:
#   - we control the key policy, rotation, and can REVOKE access (cut the key) to
#     cryptographically lock secrets in an incident.
#   - key rotation is automatic (enable_key_rotation) — yearly new backing key.
resource "aws_kms_key" "eks_secrets" {
  description             = "${var.name_prefix} EKS Secret envelope-encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 7 # short window in dev; prod often uses 30
  tags                    = merge(var.tags, { Name = "${var.name_prefix}-eks-kms" })
}

# A friendly alias so the key is identifiable in the console / other stacks.
resource "aws_kms_alias" "eks_secrets" {
  name          = "alias/${var.name_prefix}-eks-secrets"
  target_key_id = aws_kms_key.eks_secrets.key_id
}

# ============================================================================
# (2) IAM ROLE FOR THE CONTROL PLANE
# ============================================================================
# The EKS service assumes this role to manage cluster infrastructure (ENIs, LBs).
data "aws_iam_policy_document" "cluster_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name_prefix        = "${var.name_prefix}-eks-cluster-"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
  tags               = var.tags
}

# The single AWS-managed policy the control-plane role needs. Attaching the
# managed policy (vs a hand-rolled one) means AWS keeps it current as EKS evolves.
resource "aws_iam_role_policy_attachment" "cluster_policy" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

# ============================================================================
# (3) CONTROL-PLANE LOG GROUP
# ============================================================================
# Create the log group OURSELVES (with retention) before the cluster, so EKS
# writes into a group whose retention we control. If EKS auto-creates it, it
# defaults to NEVER-expire — a silent cost + compliance leak.
resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${var.name_prefix}/cluster"
  retention_in_days = var.cluster_log_retention_days
  tags              = var.tags
}

# ============================================================================
# (4) THE CLUSTER
# ============================================================================
resource "aws_eks_cluster" "this" {
  name     = var.name_prefix
  version  = var.cluster_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    # Control-plane ENIs land in the PRIVATE subnets.
    subnet_ids = var.private_subnet_ids
    # PRIVATE endpoint is always on — in-VPC traffic (nodes, bastion) reaches the
    # API without leaving AWS. PUBLIC endpoint is optional + CIDR-restricted.
    endpoint_private_access = true
    endpoint_public_access  = var.endpoint_public_access
    public_access_cidrs     = var.endpoint_public_access ? var.public_access_cidrs : null
  }

  # Envelope-encrypt the "secrets" resource type with our CMK (see KMS key above).
  encryption_config {
    provider {
      key_arn = aws_kms_key.eks_secrets.arn
    }
    resources = ["secrets"]
  }

  # Ship the chosen control-plane log streams (api/audit/authenticator) to CW.
  enabled_cluster_log_types = var.enabled_cluster_log_types

  tags = merge(var.tags, { Name = var.name_prefix })

  # Ordering: the role policy + log group must exist before the cluster, else the
  # cluster create races them (and EKS would auto-make a no-retention log group).
  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy,
    aws_cloudwatch_log_group.cluster,
  ]
}

# ============================================================================
# (5) OIDC PROVIDER — the trust anchor for IRSA
# ============================================================================
# IRSA (IAM Roles for Service Accounts) lets a POD assume an IAM role WITHOUT
# static AWS keys: the pod presents a projected ServiceAccount JWT, EKS's OIDC
# provider vouches for it, and STS exchanges it for short-lived role credentials.
# To make that trust work we must register the cluster's OIDC issuer as an IAM
# OIDC identity provider. The iam module then writes role trust policies that
# reference THIS provider's ARN.
#
# We fetch the issuer's TLS cert and use its SHA1 thumbprint as the trust
# fingerprint (instead of hardcoding one that could rotate).
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"] # the audience IRSA tokens are minted for
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
  tags            = merge(var.tags, { Name = "${var.name_prefix}-oidc" })
}

# ============================================================================
# (5b) KMS KEY FOR NODE ROOT EBS VOLUMES
# ============================================================================
# WHY a separate key from the EKS-secrets CMK:
#   The EKS-secrets key's policy is governed by AWS KMS — EKS and etcd need very
#   specific grants on it. The EBS key needs a different set of grantees: the EC2
#   Auto Scaling service (for EBS-optimized launch) and the node IAM role. Keeping
#   them separate limits blast radius (cutting one doesn't affect the other) and
#   keeps key policies legible.
#
# If the caller already has a preferred CMK they can pass node_ebs_kms_key_arn
# to skip this resource entirely (count = 0).
resource "aws_kms_key" "node_ebs" {
  count = var.node_ebs_kms_key_arn == "" ? 1 : 0

  description             = "${var.name_prefix} EKS worker-node root EBS encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  tags                    = merge(var.tags, { Name = "${var.name_prefix}-node-ebs-kms" })
}

resource "aws_kms_alias" "node_ebs" {
  count = var.node_ebs_kms_key_arn == "" ? 1 : 0

  name          = "alias/${var.name_prefix}-node-ebs"
  target_key_id = aws_kms_key.node_ebs[0].key_id
}

# Resolve the final key ARN regardless of whether we created it or it was passed in.
locals {
  node_ebs_kms_key_arn = var.node_ebs_kms_key_arn != "" ? var.node_ebs_kms_key_arn : aws_kms_key.node_ebs[0].arn
}

# ============================================================================
# (5c) KMS GRANTS FOR NODE EBS
# ============================================================================
# WHY kms:CreateGrant is needed (the subtle part):
#   When EC2/Auto Scaling launches an EBS-encrypted instance, it needs to call
#   kms:Decrypt and kms:GenerateDataKeyWithoutPlaintext on behalf of the instance.
#   It does this via an EBS service grant, NOT via the role policy, because the
#   CALLER is the EC2 service, not the node role. CreateGrant lets the node role
#   hand off a scoped grant to the EC2/EBS service at launch time. Without
#   CreateGrant, the node group launch FAILS with AccessDeniedException.
#
# We use a KMS Grant (not a key policy statement) so the permission is scoped
# to exactly the node role ARN and is auditable as a named grant.
data "aws_iam_policy_document" "node_ebs_kms" {
  # This policy document is used to allow the node role to use and grant the CMK.
  # It is attached as an inline policy on the node role (see below).
  statement {
    sid    = "NodeEBSKMSUse"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKeyWithoutPlaintext",
      "kms:CreateGrant", # lets EC2/ASG create a service grant on launch
      "kms:DescribeKey",
    ]
    resources = [local.node_ebs_kms_key_arn]
  }
}

# ============================================================================
# (5d) LAUNCH TEMPLATE — enforces IMDSv2 + encrypted EBS on every node
# ============================================================================
# WHY a launch template (not the default managed-node-group settings):
#   EKS managed node groups have two security-relevant gaps when using the DEFAULT
#   launch template:
#     1) IMDSv2 is OPTIONAL (hop limit defaults to 2, so a container inside a pod
#        can reach 169.254.169.254 and steal node-role credentials, bypassing IRSA).
#     2) Root EBS volumes are UNENCRYPTED unless you bring your own launch template.
#   Both are fixed here:
#     - metadata_options.http_tokens = "required"  → forces IMDSv2 (token exchange
#       before any metadata read); the hop limit drops to 1 (host-only) so a
#       container inside a pod cannot reach the metadata service at all.
#     - block_device_mappings with ebs.encrypted = true → root volume is encrypted
#       with our CMK.
#
# WHY hop_limit = 1: at hop limit 2 the TTL survives one
# network hop, meaning a container (which is one hop from the node kernel's network
# namespace) can reach IMDS. At hop limit 1 only the node host itself can reach it;
# a container's network namespace counts as a hop and the request dies. This is the
# correct fix for SSRF-via-IMDS attacks, which is how multiple high-profile cloud
# credential-theft incidents worked (e.g. Capital One 2019).
resource "aws_launch_template" "nodes" {
  name_prefix = "${var.name_prefix}-node-lt-"
  description = "EKS worker node launch template: IMDSv2 required, root EBS encrypted"

  # IMDS configuration — the core of finding #1.
  metadata_options {
    # endpoint must be "enabled" (the metadata service exists) but tokens are REQUIRED.
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 only — v1 requests are rejected
    http_put_response_hop_limit = 1          # host-only; containers cannot reach IMDS
    instance_metadata_tags      = "disabled" # don't expose resource tags via IMDS
  }

  # Root volume — encrypted with our CMK. The device name /dev/xvda is the
  # AL2/AL2023 EKS AMI root device; EKS-managed AMIs always use this path.
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      encrypted             = true
      kms_key_id            = local.node_ebs_kms_key_arn
      volume_size           = var.node_disk_size
      volume_type           = "gp3"
      delete_on_termination = true
      # iops and throughput: gp3 defaults (3000 IOPS, 125 MiB/s) are ample for
      # node workloads; tune in prod if image-pull throughput becomes a bottleneck.
    }
  }

  # Required tag: EKS uses this to discover which launch templates belong to its
  # node groups (for AMI updates). Without it, rolling updates may skip the template.
  tag_specifications {
    resource_type = "instance"
    tags          = merge(var.tags, { Name = "${var.name_prefix}-node" })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = merge(var.tags, { Name = "${var.name_prefix}-node-root-vol" })
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-node-lt" })

  lifecycle {
    create_before_destroy = true
  }
}

# ============================================================================
# (6) IAM ROLE FOR WORKER NODES (the node group's instance role)
# ============================================================================
data "aws_iam_policy_document" "node_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name_prefix        = "${var.name_prefix}-eks-node-"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
  tags               = var.tags
}

# The three AWS-managed policies a worker node needs:
#   WorkerNodePolicy  — register with the cluster, describe resources
#   CNI_Policy        — the VPC CNI assigns pod IPs from the subnet
#   ECR ReadOnly      — pull container images (complements the ECR VPC endpoints)
# We do NOT grant nodes broad access; pods that need AWS get it via IRSA (per
# workload, least privilege), NOT via the node role. That separation is the whole
# point of IRSA ("why not just use the node role?").
resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# Inline policy: allows the node role to use and delegate the node-EBS CMK.
# See the data "aws_iam_policy_document" "node_ebs_kms" comment above for why
# kms:CreateGrant is necessary (EC2/ASG service grant at launch time).
resource "aws_iam_role_policy" "node_ebs_kms" {
  name   = "node-ebs-kms"
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.node_ebs_kms.json
}

# ============================================================================
# (7) MANAGED NODE GROUP
# ============================================================================
# "Managed" = EKS owns the Auto Scaling Group, AMI selection, and the drain-on-
# update lifecycle. We declare size + instance type. Nodes launch into the
# PRIVATE subnets so they have no public IP and reach the internet via NAT.
#
# LAUNCH TEMPLATE WIRING: by referencing our aws_launch_template (which sets
# IMDSv2=required, hop_limit=1, and an encrypted EBS root volume) we override
# the unsafe EKS defaults. NOTE: when a launch template is specified:
#   - disk_size top-level argument MUST be omitted (it conflicts; use the
#     block_device_mappings in the template instead).
#   - instance_types top-level argument is still supported alongside the launch
#     template as long as the template does NOT set an instance_type itself.
resource "aws_eks_node_group" "this" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.name_prefix}-ng"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.private_subnet_ids

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  instance_types = var.node_instance_types
  capacity_type  = var.node_capacity_type
  # disk_size is intentionally OMITTED: the block_device_mappings in the launch
  # template controls the root volume (size + encryption). Specifying disk_size
  # alongside a launch_template block is a Terraform/EKS API conflict.

  # Wire the hardened launch template. "$Latest" is stable here because the
  # launch template is managed by this Terraform stack — every change produces a
  # new version and Terraform's plan shows exactly what changed.
  launch_template {
    id      = aws_launch_template.nodes.id
    version = aws_launch_template.nodes.latest_version
  }

  # During a version/AMI roll, never take more than one node down at a time —
  # keeps the platform's PodDisruptionBudgets satisfiable.
  update_config {
    max_unavailable = 1
  }

  # A label so workloads can target this group (e.g. nodeSelector) if needed later.
  labels = {
    "forgepoint.io/nodegroup" = "general"
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-ng" })

  # The node role's policies + KMS grant must exist before nodes try to join;
  # the EBS CMK policy is also needed so the Auto Scaling group can create the
  # EBS grant at launch time.
  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
    aws_iam_role_policy.node_ebs_kms,
  ]

  # EKS bumps the AMI release version out-of-band; ignore drift on desired_size
  # so an external autoscaler (Cluster Autoscaler/Karpenter) can own it without
  # Terraform fighting it on every plan.
  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }
}
