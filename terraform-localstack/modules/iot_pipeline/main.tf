resource "aws_kinesis_stream" "iot_stream" {
  name             = var.kinesis_stream_name
  shard_count      = 2
  retention_period = 24
}

resource "aws_dynamodb_table" "iot_data_table" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "device_id"
    type = "S"
  }
  attribute {
    name = "timestamp"
    type = "N"
  }
  hash_key = "device_id"  # Partition Key
  range_key = "timestamp"  # Sort Key
}