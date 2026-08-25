# paimon-s3 requires static S3 access-key/secret-key (it can't use the IRSA IAM-role
# chain). So we create a dedicated IAM user scoped to ONLY the two buckets, and stash
# its key in SSM SecureString. The per-run workflow (OIDC) reads SSM and injects a
# k8s Secret — no static secret ever lives in the repo or GitHub.
#
# Everything else (Iceberg/Delta Spark, the pods' general S3) stays on IRSA. This user
# is only for the Paimon-based stacks (Fluss + flink-paimon).
resource "aws_iam_user" "paimon_s3" {
  name = "${var.project_tag}-paimon-s3"
}

resource "aws_iam_user_policy" "paimon_s3" {
  name = "buckets"
  user = aws_iam_user.paimon_s3.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
      Resource = [
        "arn:aws:s3:::${var.paimon_bucket}", "arn:aws:s3:::${var.paimon_bucket}/*",
        "arn:aws:s3:::${var.warehouse_bucket}", "arn:aws:s3:::${var.warehouse_bucket}/*"
      ]
    }]
  })
}

resource "aws_iam_access_key" "paimon_s3" {
  user = aws_iam_user.paimon_s3.name
}

# SSM SecureString — encrypted at rest; the workflow reads these via the deploy role.
resource "aws_ssm_parameter" "paimon_s3_key_id" {
  name  = "/${var.project_tag}/paimon-s3/access-key-id"
  type  = "SecureString"
  value = aws_iam_access_key.paimon_s3.id
}

resource "aws_ssm_parameter" "paimon_s3_secret" {
  name  = "/${var.project_tag}/paimon-s3/secret-access-key"
  type  = "SecureString"
  value = aws_iam_access_key.paimon_s3.secret
}

output "paimon_s3_ssm_prefix" {
  value = "/${var.project_tag}/paimon-s3"
}
