output "kinesis_stream_arn" {
  value = aws_kinesis_stream.iot_stream.arn
}
output "dynamodb_table_name" {
  value = aws_dynamodb_table.iot_data_table.name
}