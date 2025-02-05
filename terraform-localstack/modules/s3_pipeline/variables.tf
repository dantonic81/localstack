variable "landing_bucket_name" {
  description = "Name of the landing (staging) S3 bucket"
  type        = string
}

variable "prod_bucket_name" {
  description = "Name of the production S3 bucket"
  type        = string
}