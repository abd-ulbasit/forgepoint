# ============================================================================
# github-oidc — a short-lived, keyless identity for GitHub Actions to push to ECR
# ============================================================================
# HOW IT WORKS: a workflow job with `permissions: id-token: write` asks GitHub
# for an OIDC token (issuer https://token.actions.githubusercontent.com) and
# exchanges it with sts:AssumeRoleWithWebIdentity for this role (e.g. via
# aws-actions/configure-aws-credentials with role-to-assume: vars.AWS_ROLE_ARN).
# STS checks the token against the trust policy below, so no AWS access key is
# ever stored in GitHub.
#
# THE LOCK is the `sub` claim. With GitHub's default claim template it looks
# like:
#   repo:abd-ulbasit/forgepoint:ref:refs/heads/main      (push to main)
#   repo:abd-ulbasit/forgepoint:ref:refs/tags/v1.2.3     (tag push)
#   repo:abd-ulbasit/forgepoint:pull_request             (any PR run)
# Allowing only main and v* tags means a pull_request run, including from a
# fork, can never assume the role. Repositories created, renamed or transferred
# after 15 Jul 2026 get the immutable form instead
# (repo:OWNER@OWNER_ID/REPO@REPO_ID:...), so check a real token's sub before
# relying on the default list.
# ----------------------------------------------------------------------------

# One GitHub OIDC provider per AWS account. If the account already has one
# (another repo set it up), pass its ARN and set create_oidc_provider = false.
resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_oidc_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # No thumbprint_list: IAM validates GitHub's issuer against its own trusted
  # CA store, and the provider no longer requires a pinned thumbprint.
  tags = var.tags
}

locals {
  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : var.oidc_provider_arn
}

data "aws_iam_policy_document" "trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    # Audience must be STS (what configure-aws-credentials requests).
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Subject must be one of the allowed refs. StringLike so a tag pattern
    # (refs/tags/v*) works; there is deliberately no bare "repo:...:*".
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = var.allowed_subjects
    }
  }
}

resource "aws_iam_role" "ecr_push" {
  name                 = "${var.name_prefix}-github-ecr-push"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
  tags                 = var.tags
}

data "aws_iam_policy_document" "ecr_push" {
  # GetAuthorizationToken is account-level; it cannot be scoped to a repo.
  statement {
    sid       = "ECRLogin"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  # Push (and the reads a push needs) on the listed repositories only.
  statement {
    sid    = "ECRPush"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
      "ecr:PutImage",
    ]
    resources = var.ecr_repository_arns
  }
}

resource "aws_iam_role_policy" "ecr_push" {
  name   = "ecr-push"
  role   = aws_iam_role.ecr_push.id
  policy = data.aws_iam_policy_document.ecr_push.json
}
