# ============================================================================
# REMOTE STATE BACKEND — PATTERN / REFERENCE (root level)
# ============================================================================
# This file documents the backend pattern for the whole tree. The ACTUAL backend
# config lives per-environment in environments/<env>/backend.tf, because each
# environment must keep its state in a SEPARATE key (and ideally a separate
# bucket/account) so a `terraform destroy` in dev can never touch prod state.
#
# WHY remote state at all:
#   - SHARED + LOCKED: a team (or CI) runs Terraform from many machines. Local
#     state on one laptop is a single point of truth/failure and invites two
#     people applying at once and corrupting state. S3 stores it centrally;
#     DynamoDB provides a LOCK so only one apply mutates state at a time.
#   - DURABLE + VERSIONED: the S3 bucket has versioning on (see the state-backend
#     module), so a botched apply can be rolled back to a prior state object.
#   - ENCRYPTED: state contains sensitive material (resource IDs, sometimes
#     secret values), so the bucket is SSE-encrypted and the backend sets
#     `encrypt = true`.
#
# CHICKEN-AND-EGG: the S3 bucket + DynamoDB table that back this state are
# themselves created by modules/state-backend. Bootstrap once with LOCAL state,
# then `terraform init -migrate-state` into S3. See README.md "Bootstrapping".
#
# This block is intentionally COMMENTED OUT here. The root dir is not a rootmodule
# you `apply` — it just holds shared versions.tf + this reference. The real,
# uncommented (still example-valued) block is in environments/dev/backend.tf.
# ----------------------------------------------------------------------------
#
# terraform {
#   backend "s3" {
#     bucket         = "forgepoint-tfstate-<account_id>"   # from state-backend module output
#     key            = "<env>/terraform.tfstate"           # per-env key (dev/, staging/, prod/)
#     region         = "us-east-1"                         # bucket region (not necessarily infra region)
#     dynamodb_table = "forgepoint-tflock"                 # lock table from state-backend module
#     encrypt        = true                                # encrypt the state object at rest
#   }
# }
