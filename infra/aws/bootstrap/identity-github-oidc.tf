# GitHub Actions → AWS via OIDC (no static access keys). Standing: the run assumes
# this deploy role; destroying the run must not delete it.
data "aws_partition" "current" {}

# One OIDC provider per account. If it already exists, import it once:
#   terraform import aws_iam_openid_connect_provider.github \
#     arn:aws:iam::<account-id>:oidc-provider/token.actions.githubusercontent.com
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [for s in var.github_subjects : "repo:${var.github_repo}:${s}"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name                 = "${var.project_tag}-gha-deploy"
  assume_role_policy   = data.aws_iam_policy_document.github_trust.json
  max_session_duration = 3600 * 3
}

resource "aws_iam_role_policy" "github_deploy" {
  name = "deploy"
  role = aws_iam_role.github_deploy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ProvisionRun"
        Effect = "Allow"
        Action = [
          "eks:*", "ec2:*", "elasticloadbalancing:*", "autoscaling:*",
          "iam:*", "ecr:*", "logs:*", "events:*", "scheduler:*", "lambda:*",
          "codebuild:*", "sqs:*", "elasticfilesystem:*", "dynamodb:*",
          "glue:*", "athena:*", "cloudwatch:*", "budgets:*",
          "ssm:GetParameter", "ssm:GetParameters", "kms:Decrypt"
        ]
        Resource = "*"
      },
      {
        Sid    = "Buckets"
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [
          "arn:${data.aws_partition.current.partition}:s3:::${var.warehouse_bucket}",
          "arn:${data.aws_partition.current.partition}:s3:::${var.warehouse_bucket}/*",
          "arn:${data.aws_partition.current.partition}:s3:::${var.paimon_bucket}",
          "arn:${data.aws_partition.current.partition}:s3:::${var.paimon_bucket}/*"
        ]
      }
    ]
  })
}

output "github_deploy_role_arn" {
  value = aws_iam_role.github_deploy.arn
}
