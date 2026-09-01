# STANDING resources, applied ONCE, never torn down per run: GitHub OIDC deploy
# role, the tflock table, and ECR repos. The ephemeral run (../) references these.
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.40" }
  }
  backend "s3" {
    bucket  = "streaming-comparison-amc-warehouse"
    key     = "tfstate/bootstrap.tfstate"
    region  = "eu-west-1"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = { Project = var.project_tag } }
}
