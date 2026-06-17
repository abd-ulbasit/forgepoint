# ============================================================================
# ecr — one repository per service, scan-on-push, immutable tags, lifecycle expiry
# ============================================================================
# for_each over the service set turns the list into a map keyed by service name,
# so each repo is addressable as aws_ecr_repository.this["auth"] etc. — stable
# addresses that don't shift if the list is reordered (unlike count + index).
# ----------------------------------------------------------------------------

locals {
  # toset() gives for_each a set; the key IS the service name.
  services = toset(var.service_names)
}

resource "aws_ecr_repository" "this" {
  for_each = local.services

  name                 = "${var.name_prefix}/${each.value}"
  image_tag_mutability = var.image_tag_mutability
  # force_delete lets dev teardown remove repos that still hold images.
  force_delete = var.force_delete

  image_scanning_configuration {
    scan_on_push = var.scan_on_push
  }

  # Encrypt image layers at rest. KMS would let us use a CMK; AES256 (S3-managed)
  # is the zero-config default and adequate for image bytes (which are not
  # secrets the way DB creds are). Choosing AES256 keeps it simple + free.
  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}/${each.value}" })
}

# LIFECYCLE POLICY — applied per repo. Two rules, evaluated by rulePriority:
#   1) expire untagged images older than N days  (reclaim orphaned manifests)
#   2) keep only the newest M tagged images       (cap release-image storage)
# Lifecycle policies are JSON; we encode them with jsonencode for readability and
# so values come from variables (no magic numbers baked into a heredoc).
resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = aws_ecr_repository.this
  repository = each.value.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after ${var.untagged_expiry_days} days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = var.untagged_expiry_days
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep only the newest ${var.max_tagged_images} tagged images"
        selection = {
          tagStatus = "any"
          countType = "imageCountMoreThan"
          # When tagStatus=any, ECR keeps the newest countNumber images and
          # expires the rest — our cap on accumulated releases.
          countNumber = var.max_tagged_images
        }
        action = { type = "expire" }
      },
    ]
  })
}
