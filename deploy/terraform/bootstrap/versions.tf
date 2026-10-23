# ============================================================================
# bootstrap — VERSION PINS
# ============================================================================
# Same AWS provider constraint as environments/*/versions.tf. The committed
# .terraform.lock.hcl freezes the exact build and its checksums.
#
# required_version >= 1.11: the environments' S3 backend uses use_lockfile
# (S3-native state locking), which is GA from Terraform 1.11.
# ============================================================================

terraform {
  required_version = ">= 1.11.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}
