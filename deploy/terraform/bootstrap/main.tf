# ============================================================================
# bootstrap — creates the remote-state backend for every other root
# ============================================================================
# This is the ONE root that starts with local state, because the bucket it
# creates is where everything else keeps its state. Run it once per account:
#
#   terraform -chdir=deploy/terraform/bootstrap init
#   terraform -chdir=deploy/terraform/bootstrap apply
#
# Then copy the `backend_config` output into environments/<env>/backend.tf (or
# a gitignored *.backend.hcl passed with -backend-config) and run
# `terraform init -migrate-state` there. Optionally migrate this root's own
# state into the bucket too (key = "bootstrap/terraform.tfstate"): the bucket
# and key have prevent_destroy, so losing the local file is recoverable with
# `terraform import`, but keeping it remote is tidier.
#
# Everything here is protected from `terraform destroy` (prevent_destroy on the
# bucket and the KMS key). Tearing the backend down is a deliberate, manual act.
# ============================================================================

data "aws_caller_identity" "current" {}

module "state_backend" {
  source = "../modules/state-backend"

  # Bucket names are global, so suffix with the account id (read at plan time,
  # never hard-coded).
  bucket_name                 = "forgepoint-tfstate-${data.aws_caller_identity.current.account_id}"
  state_access_principal_arns = var.state_access_principal_arns
}
