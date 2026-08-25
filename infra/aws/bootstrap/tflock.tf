# Terraform state lock table (referenced by both bootstrap and the run backends).
resource "aws_dynamodb_table" "tflock" {
  name         = "${var.project_tag}-tflock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"
  attribute {
    name = "LockID"
    type = "S"
  }
}
