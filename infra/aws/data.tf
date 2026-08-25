# References to the STANDING bootstrap resources (created once by ../bootstrap).
data "aws_iam_role" "github_deploy" {
  name = "${var.project_tag}-gha-deploy"
}

locals {
  ecr_registry = "${var.aws_account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}
