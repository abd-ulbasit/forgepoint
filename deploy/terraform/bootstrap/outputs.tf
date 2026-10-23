output "state_bucket_name" {
  description = "The remote-state bucket."
  value       = module.state_backend.state_bucket_name
}

output "kms_key_arn" {
  description = "The KMS key that encrypts state objects."
  value       = module.state_backend.kms_key_arn
}

output "backend_config" {
  description = "Backend settings for environments/<env>/backend.tf (replace <env>). Not secret: bucket and key names only."
  value       = <<-EOT
    bucket       = "${module.state_backend.state_bucket_name}"
    key          = "<env>/terraform.tfstate"
    region       = "${var.aws_region}"
    encrypt      = true
    kms_key_id   = "${module.state_backend.kms_key_arn}"
    use_lockfile = true
  EOT
}
