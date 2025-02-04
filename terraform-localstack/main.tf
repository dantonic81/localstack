provider "aws" {
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "http://localhost:4566"
  }
}

# Staging (Landing) bucket
resource "aws_s3_bucket" "landing_bucket" {
  bucket = "landing"
}

resource "aws_s3_bucket_acl" "landing_bucket_acl" {
  bucket = aws_s3_bucket.landing_bucket.id
  acl    = "private"
}

# Production bucket
resource "aws_s3_bucket" "prod_bucket" {
  bucket = "prod"
}

resource "aws_s3_bucket_acl" "prod_bucket_acl" {
  bucket = aws_s3_bucket.prod_bucket.id
  acl    = "private"
}

output "landing_bucket_name" {
  value = aws_s3_bucket.landing_bucket.bucket
}

output "prod_bucket_name" {
  value = aws_s3_bucket.prod_bucket.bucket
}
