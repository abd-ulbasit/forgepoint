variable "aws_region" {
  description = "Region for the state bucket and its KMS key. Every environment's backend block must name the same region."
  type        = string
  default     = "us-east-1"
}

variable "state_access_principal_arns" {
  description = "IAM principals (wildcards allowed) allowed to read/write state objects. Non-empty = everyone else is denied by the bucket policy. Example: [\"arn:aws:iam::<account>:role/terraform-*\", \"arn:aws:iam::<account>:user/<you>\"]. Include whatever identity runs this bootstrap if you plan to migrate its own state into the bucket."
  type        = list(string)
  default     = []
}
