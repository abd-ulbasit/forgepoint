# ============================================================================
# s3 — model-artifacts bucket: versioned, SSE-KMS, public-access BLOCKED, policy, lifecycle
# ============================================================================
# Every hardening control the task asks for, each as its own resource (the modern
# AWS provider splits bucket config into discrete resources rather than one giant
# inline block — easier to read and to reason about per-control).
# ----------------------------------------------------------------------------

resource "aws_s3_bucket" "artifacts" {
  bucket = var.bucket_name
  tags   = merge(var.tags, { Name = var.bucket_name })
}

# VERSIONING: model weights are immutable history. If a bad artifact is uploaded
# under an existing key, the previous good version is still retrievable. Also the
# safety net behind the registry's "pin a model version to an exact artifact".
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-KMS by default. If a KMS key ARN was supplied we use aws:kms with that CMK
# (customer-controlled key, auditable via CloudTrail). If not, fall back to SSE-S3
# (AES256) — still encrypted at rest, just with the AWS-managed key. bucket_key
# reduces KMS API calls (and cost) by caching a bucket-level data key.
resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }
    bucket_key_enabled = var.kms_key_arn != ""
  }
}

# PUBLIC ACCESS BLOCK — all four guards. Model artifacts must never be world-
# readable. This blocks any ACL or policy that would make an object/bucket public,
# even if one is added later by mistake.
resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# OWNERSHIP CONTROLS — BucketOwnerEnforced disables ACLs entirely (the AWS-
# recommended modern default). Access is governed purely by IAM + bucket policy,
# which is simpler to audit than ACL + policy interactions.
resource "aws_s3_bucket_ownership_controls" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# LIFECYCLE — control storage cost without losing recoverability:
#   1) expire NONCURRENT versions after N days (don't keep every overwrite forever)
#   2) abort orphaned multipart uploads (failed big-artifact uploads leak storage)
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    # Apply to the whole bucket (empty prefix filter). Required by the provider's
    # newer schema to have an explicit filter block.
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_days
    }
  }
}

# ----------------------------------------------------------------------------
# BUCKET POLICY — defense-in-depth: deny anything not using TLS.
# ----------------------------------------------------------------------------
# IAM (via IRSA in the iam module) controls WHO can read/write. This policy adds a
# transport guarantee on top: any request over plain HTTP (aws:SecureTransport
# false) is DENIED outright. So even a correctly-authorized caller can't
# accidentally pull/push model weights in cleartext. A standard S3 hardening
# baseline reviewers look for.
data "aws_iam_policy_document" "artifacts" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  policy = data.aws_iam_policy_document.artifacts.json

  # The public-access-block must be in place first, or attaching a policy that
  # references a "*" principal can transiently trip the block-public-policy guard.
  depends_on = [aws_s3_bucket_public_access_block.artifacts]
}
