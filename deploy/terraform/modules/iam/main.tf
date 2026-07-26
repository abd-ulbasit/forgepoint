# ============================================================================
# iam — IRSA roles: trust policy bound to a K8s ServiceAccount via OIDC, +
#       least-privilege permission policies (S3 artifacts, Secrets Manager, KMS)
# ============================================================================
#
# HOW IRSA WORKS:
#   1. The pod's ServiceAccount is annotated eks.amazonaws.com/role-arn=<this role>.
#   2. EKS projects a signed OIDC JWT (audience sts.amazonaws.com) into the pod.
#   3. The AWS SDK in the pod calls sts:AssumeRoleWithWebIdentity with that JWT.
#   4. STS validates the JWT against the cluster's OIDC PROVIDER (registered in
#      the eks module) and checks this role's TRUST POLICY conditions:
#         - aud == sts.amazonaws.com
#         - sub == system:serviceaccount:<namespace>:<sa-name>   ← the lock
#   5. If both match, STS returns short-lived creds scoped to this role.
#
#   The `sub` condition is what makes it least-privilege: ONLY a pod running as
#   exactly that ServiceAccount in that namespace can assume the role. A pod in a
#   different namespace, or using a different SA, is rejected by STS.
# ----------------------------------------------------------------------------

# Build the per-role TRUST policy. The two StringEquals conditions on the OIDC
# `:sub` and `:aud` keys are the heart of IRSA — see the block comment above.
data "aws_iam_policy_document" "trust" {
  for_each = var.roles

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    # Pin to exactly this ServiceAccount. system:serviceaccount:<ns>:<name> is the
    # `sub` claim EKS puts in the projected token.
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${each.value.namespace}:${each.value.service_account_name}"]
    }

    # Pin the audience — defends against a token minted for a different audience
    # being replayed here.
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "irsa" {
  for_each           = var.roles
  name               = "${var.name_prefix}-irsa-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.trust[each.key].json
  tags               = merge(var.tags, { Name = "${var.name_prefix}-irsa-${each.key}" })
}

# ----------------------------------------------------------------------------
# Per-role PERMISSION policy, assembled from the role spec's toggles.
# ----------------------------------------------------------------------------
# We build ONE inline policy document per role from dynamic statements so a role
# only ever gets the exact statements it needs. Roles with no access produce an
# empty document, which we skip (no empty policy attached).
data "aws_iam_policy_document" "permissions" {
  for_each = var.roles

  # --- S3 model-artifacts access (read or readwrite) ----------------------
  # List on the bucket; object actions on its contents. Scoped to the artifacts
  # bucket ARN only — never s3:* on "*".
  dynamic "statement" {
    # Emit only if this role requested S3 access AND a bucket ARN was provided.
    for_each = (each.value.s3_artifact_access != "" && var.artifacts_bucket_arn != "") ? [each.value.s3_artifact_access] : []
    content {
      sid    = "S3ArtifactAccess"
      effect = "Allow"
      actions = statement.value == "readwrite" ? [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        ] : [
        # read-only
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
      ]
      resources = [
        var.artifacts_bucket_arn,
        "${var.artifacts_bucket_arn}/*",
      ]
    }
  }

  # --- Secrets Manager: GetSecretValue on the EXACT secret ARNs listed -----
  # This is how a service reads its DB/Redis credentials at runtime (directly, or
  # via External Secrets Operator running under its own IRSA role). Scoped to the
  # specific secret ARNs — not secretsmanager:* on all secrets.
  dynamic "statement" {
    for_each = length(each.value.secret_arns) > 0 ? [1] : []
    content {
      sid    = "ReadSecrets"
      effect = "Allow"
      actions = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret",
      ]
      resources = each.value.secret_arns
    }
  }

  # --- KMS Decrypt on the keys that wrap those secrets / S3 objects --------
  # Reading a KMS-encrypted secret or SSE-KMS S3 object requires kms:Decrypt on
  # the wrapping key. Scoped to the specific key ARNs the workload needs.
  dynamic "statement" {
    for_each = length(each.value.kms_key_arns) > 0 ? [1] : []
    content {
      sid    = "KMSDecrypt"
      effect = "Allow"
      actions = [
        "kms:Decrypt",
        "kms:DescribeKey",
      ]
      resources = each.value.kms_key_arns
    }
  }
}

# Attach the assembled policy — but only for roles that actually got at least one
# statement. `json` for an empty document is `{"Version":...,"Statement":[]}`;
# attaching that is harmless but noisy, so we filter to roles with real access.
locals {
  roles_with_permissions = {
    for k, v in var.roles : k => v
    if(v.s3_artifact_access != "" && var.artifacts_bucket_arn != "") || length(v.secret_arns) > 0 || length(v.kms_key_arns) > 0
  }
}

resource "aws_iam_role_policy" "irsa" {
  for_each = local.roles_with_permissions
  name     = "permissions"
  role     = aws_iam_role.irsa[each.key].id
  policy   = data.aws_iam_policy_document.permissions[each.key].json
}
