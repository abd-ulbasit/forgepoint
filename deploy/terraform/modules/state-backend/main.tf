# ============================================================================
# state-backend — the S3 bucket + DynamoDB lock table that back REMOTE STATE
# ============================================================================
# Every other stack stores its state in THIS bucket and locks against THIS table.
# The bucket therefore gets the full hardening treatment: versioned (roll back a
# bad apply), encrypted (state can hold secrets), and public access fully blocked.
# ----------------------------------------------------------------------------

# The state bucket itself. Note: no `acl` argument — public-block (below) +
# bucket-owner-enforced object ownership is the modern, ACL-free way to keep a
# bucket private.
resource "aws_s3_bucket" "state" {
  bucket = var.bucket_name
  tags   = var.tags
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

# ENCRYPTION AT REST: SSE with the S3-managed key (SSE-S3 / AES256). We use the
# AWS-managed key here rather than a customer KMS key on purpose — the state
# bucket must be usable during bootstrap BEFORE any KMS key exists, and a KMS
# key whose own state lives in this bucket would be circular. SSE-S3 still
# encrypts everything at rest with zero key management.
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    # bucket_key_enabled has no effect for AES256; omitted deliberately.
  }
}

# PUBLIC ACCESS BLOCK: all four switches on. State is the most sensitive thing in
# the repo — it must NEVER be reachable from the internet, even by accident via a
# stray ACL or bucket policy. This object is the belt-and-suspenders guard.
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB LOCK TABLE. The S3 backend uses this for advisory locking: before an
# apply mutates state it writes a lock item keyed by `LockID`; concurrent applies
# see the lock and refuse, preventing two writers from racing on one state file.
#   - PAY_PER_REQUEST (on-demand): locking traffic is tiny + bursty; on-demand
#     costs cents and needs no capacity planning vs provisioned throughput.
#   - hash key MUST be named exactly "LockID" (string) — that's the contract the
#     S3 backend expects; renaming it breaks locking.
resource "aws_dynamodb_table" "lock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  # Recover the table to any point in the last 35 days if it's corrupted/deleted.
  # Cheap insurance for the component that guards every other apply.
  point_in_time_recovery {
    enabled = true
  }

  # ENCRYPTION AT REST — SSE with the AWS-owned KMS key (SSE_OWNED_KEY). Like the
  # S3 state bucket we deliberately avoid a customer CMK here because this table
  # must be usable BEFORE any application CMK exists (bootstrap circular-dependency
  # problem). AWS-owned encryption still satisfies CIS DynamoDB controls (encrypted
  # at rest); it simply uses an AWS-managed root key rather than a customer one.
  server_side_encryption {
    enabled = true
  }

  tags = var.tags
}
