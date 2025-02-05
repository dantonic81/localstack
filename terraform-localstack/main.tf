module "s3_pipeline" {
  source               = "./modules/s3_pipeline"
  landing_bucket_name  = "landing"
  prod_bucket_name     = "prod"
}

module "iot_pipeline" {
  source               = "./modules/iot_pipeline"
  kinesis_stream_name  = "iot_data_stream"
  dynamodb_table_name  = "iot_data_table"
}