terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.11"
    }
  }

  # Durable state so the kill-switch / orphan sweep can always find + destroy a
  # run's resources. Create the lock table once (see infra/aws/README.md bootstrap).
  backend "s3" {
    bucket         = "streaming-comparison-amc-warehouse"
    key            = "tfstate/eks-run.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "streaming-comparison-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project_tag
      RunId   = var.run_id
    }
  }
}
