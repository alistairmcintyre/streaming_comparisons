# Workload IRSA role: pods (Fluss, Flink, Spark, generator) that touch S3/Glue/Athena
# assume this via a service-account annotation — no static keys. The Fluss server
# entrypoint already drops blank S3 keys → default (IAM) credential chain, so IRSA
# "just works" once the SA is annotated with this role ARN.
data "aws_iam_policy_document" "workload_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "${module.eks.oidc_provider}:sub"
      values   = [for ns in var.workload_namespaces : "system:serviceaccount:${ns}:*"]
    }
  }
}

resource "aws_iam_role" "workload" {
  name               = "${local.name}-workload"
  assume_role_policy = data.aws_iam_policy_document.workload_trust.json
}

resource "aws_iam_role_policy" "workload" {
  name = "lake-access"
  role = aws_iam_role.workload.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Buckets"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::${var.paimon_bucket}", "arn:aws:s3:::${var.paimon_bucket}/*",
          "arn:aws:s3:::${var.warehouse_bucket}", "arn:aws:s3:::${var.warehouse_bucket}/*"
        ]
      },
      {
        Sid      = "GlueCatalog"
        Effect   = "Allow"
        Action   = ["glue:*Database*", "glue:*Table*", "glue:*Partition*", "glue:GetCatalog*", "glue:BatchCreatePartition", "glue:BatchGetPartition"]
        Resource = "*"
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup", "athena:StopQueryExecution"]
        Resource = "*"
      }
    ]
  })
}

output "workload_role_arn" {
  description = "Annotate lake-accessing service accounts with eks.amazonaws.com/role-arn = this."
  value       = aws_iam_role.workload.arn
}
