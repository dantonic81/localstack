output "landing_bucket_name" {
  value = aws_s3_bucket.landing_bucket.bucket
}

output "prod_bucket_name" {
  value = aws_s3_bucket.prod_bucket.bucket
}
