# ============================================================================
# state-backend — the S3 bucket (and its KMS key) that back REMOTE STATE
# ============================================================================
# Every other stack stores its state in THIS bucket. State is the most sensitive
# artifact in the repo: it holds every resource attribute, including the Redis
# AUTH token (see modules/elasticache) and anything else a provider persists. So
# the bucket gets the full treatment: versioned, encrypted with a customer-managed
# KMS key, TLS-only, public access blocked, ACLs disabled, and protected from
# `terraform destroy`.
#
# LOCKING: S3-native lock files (`use_lockfile = true` in the backend block,
# GA since Terraform 1.11). Terraform writes <key>.tflock next to the state
# object with a conditional PUT, so there is no DynamoDB table any more; the
# DynamoDB lock is deprecated in the S3 backend.
#
# Created ONCE, by the bootstrap root (deploy/terraform/bootstrap), which starts
# with local state. See README.md "Bootstrapping order".
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# KMS key for the state objects (Trivy AWS-0132: S3 encryption should use a CMK).
# ----------------------------------------------------------------------------
# WHY a CMK rather than SSE-S3: reading state then needs kms:Decrypt on THIS key
# as well as s3:GetObject, so a broad S3 read grant elsewhere in the account no
# longer exposes state, and every decrypt is a CloudTrail event.
#
# The old "no CMK, to avoid a bootstrap circular dependency" argument does not
# hold: the key and the bucket are created together by the bootstrap root (local
# state), before any stack stores state in the bucket. The real hazard is
# losing the key, which would make every state object unreadable, hence
# prevent_destroy and the maximum 30-day deletion window.
resource "aws_kms_key" "state" {
  description             = "Terraform remote state encryption key (${var.bucket_name})"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  tags                    = var.tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "state" {
  name          = "alias/${var.bucket_name}"
  target_key_id = aws_kms_key.state.key_id
}

# The state bucket itself. No `acl` argument: public-block (below) +
# BucketOwnerEnforced ownership is the modern, ACL-free way to keep it private.
resource "aws_s3_bucket" "state" {
  bucket = var.bucket_name
  tags   = var.tags

  # A `terraform destroy` (or a refactor that drops this resource) must not be
  # able to delete the bucket holding every environment's state.
  lifecycle {
    prevent_destroy = true
  }
}

# VERSIONING: keep every prior version of the state object. If an apply corrupts
# state or someone fat-fingers a destroy, you can recover the previous state file
# from S3 version history. This is the single most important setting on a state
# bucket — do NOT disable it.
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ENCRYPTION AT REST: SSE-KMS with the CMK above. bucket_key_enabled caches a
# bucket-level data key, which cuts KMS request cost. The backend block should
# also set kms_key_id to this key (see environments/dev/backend.tf); without it
# the S3 backend asks for SSE-S3 explicitly on each write.
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.state.arn
    }
    bucket_key_enabled = true
  }
}

# PUBLIC ACCESS BLOCK: all four switches on. State must NEVER be reachable from
# the internet, even by accident via a stray ACL or bucket policy.
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# OWNERSHIP: BucketOwnerEnforced disables ACLs entirely, so access is decided by
# IAM + the bucket policy alone.
resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# ----------------------------------------------------------------------------
# BUCKET POLICY
# ----------------------------------------------------------------------------
#   1. DenyInsecureTransport: any request over plain HTTP is refused, for every
#      principal (state must never cross the wire unencrypted).
#   2. DenyStateAccessToOthers (only when state_access_principal_arns is set):
#      object reads/writes/deletes are refused to every principal NOT in the
#      list. Bucket-level actions (e.g. PutBucketPolicy) are deliberately left
#      out of the deny so an account admin can never be locked out of fixing
#      this policy.
data "aws_iam_policy_document" "state" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.state.arn,
      "${aws_s3_bucket.state.arn}/*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  dynamic "statement" {
    for_each = length(var.state_access_principal_arns) > 0 ? [1] : []
    content {
      sid    = "DenyStateAccessToOthers"
      effect = "Deny"
      principals {
        type        = "*"
        identifiers = ["*"]
      }
      actions = [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
      ]
      resources = ["${aws_s3_bucket.state.arn}/*"]
      condition {
        test     = "ArnNotLike"
        variable = "aws:PrincipalArn"
        values   = var.state_access_principal_arns
      }
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = data.aws_iam_policy_document.state.json

  # The public-access block must be in place first, or attaching a policy that
  # references a "*" principal can transiently trip the block-public-policy guard.
  depends_on = [aws_s3_bucket_public_access_block.state]
}
