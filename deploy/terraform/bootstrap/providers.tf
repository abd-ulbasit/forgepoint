provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "forgepoint"
      Environment = "shared"
      ManagedBy   = "terraform"
      Stack       = "bootstrap"
    }
  }
}
