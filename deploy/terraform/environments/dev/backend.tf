# ============================================================================
# dev — REMOTE STATE BACKEND
# ============================================================================
# State for the DEV environment lives under its own key in the shared state
# bucket (created by deploy/terraform/bootstrap via modules/state-backend).
# Separate key per env => a dev destroy can never touch staging/prod state.
#
# WHY values are commented + placeholder:
#   - The bucket and key don't exist until the bootstrap root has been applied
#     (see README "Bootstrapping order"). Until then we run with LOCAL state.
#   - CI (.github/workflows/terraform.yml) validates OFFLINE with
#     `terraform init -backend=false`, which SKIPS this block entirely (no AWS
#     creds, no real bucket). CI does not plan: there are no AWS credentials in
#     CI and this stack has never been applied.
#
# TO GO LIVE: uncomment and fill in from the bootstrap root's `backend_config`
# output (or put the same lines in a gitignored dev.backend.hcl and pass
# -backend-config=dev.backend.hcl), then `terraform init -migrate-state`.
#
# LOCKING: use_lockfile = true (S3-native lock file, GA in Terraform 1.11). The
# old dynamodb_table setting is deprecated and the module no longer creates a
# lock table. kms_key_id makes each state write use the state CMK; without it
# the backend asks S3 for SSE-S3 on every write.
# Backend config can't use variables/interpolation (a Terraform limitation — the
# backend is initialized before the rest of the config is evaluated), so these are
# literal strings or supplied via `-backend-config=...` flags / a *.hcl file.
# ----------------------------------------------------------------------------

terraform {
  backend "s3" {
    # bucket       = "forgepoint-tfstate-<account_id>"
    # key          = "dev/terraform.tfstate"
    # region       = "us-east-1"
    # encrypt      = true
    # kms_key_id   = "<state kms_key_arn from the bootstrap output>"
    # use_lockfile = true
  }
}
