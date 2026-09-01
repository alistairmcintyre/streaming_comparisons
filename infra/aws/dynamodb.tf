# DynamoDB table for the Delta S3DynamoDBLogStore, serializes concurrent Delta
# commits on S3 (streaming writer + OPTIMIZE/VACUUM maintenance). Per-run, on-demand
# billing, deleted at teardown.
resource "aws_dynamodb_table" "delta_logstore" {
  name         = "${local.name}-delta-logstore"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "tablePath"
  range_key    = "fileName"

  attribute {
    name = "tablePath"
    type = "S"
  }
  attribute {
    name = "fileName"
    type = "S"
  }
}

output "delta_logstore_table" {
  value = aws_dynamodb_table.delta_logstore.name
}
