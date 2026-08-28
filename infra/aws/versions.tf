terraform {
  # 1.10+ REQUIRED: the backend below uses `use_lockfile`, S3-native state locking,
  # which older Terraform silently does not understand.
  required_version = ">= 1.10"

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
  # run's resources.
  # LOCKING: `use_lockfile` (S3 conditional writes) rather than `dynamodb_table`, which
  # Terraform deprecated — it warned on every plan, apply and destroy. The lock is now a
  # .tflock object beside the state in the same bucket, so the separate DynamoDB lock
  # table is no longer part of the critical path. Requires Terraform >= 1.10, which is why
  # required_version moved and why killswitch.tf no longer pins 1.9.8.
  backend "s3" {
    bucket       = "streaming-comparison-amc-warehouse"
    key          = "tfstate/eks-run.tfstate"
    region       = "eu-west-1"
    use_lockfile = true
    encrypt      = true
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
