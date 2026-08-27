# Strict 2.5h cutoff — an AWS-side dead-man's switch that does NOT depend on the
# GitHub Actions runner (or your laptop) staying alive.
#
#   time_offset (apply time + 150m)  ─►  EventBridge Scheduler one-time  ─►
#   CodeBuild "teardown"  ─►  (1) terminate cluster EC2 (Karpenter + nodegroup
#   orphans, stops the compute bleed immediately)  (2) terraform destroy.
#
# terraform destroy is used (not hand-rolled deletion) so resource dependency
# ordering is correct. The normal end-of-run teardown is the GitHub Actions
# `destroy` job; this fires only if that never happens.

data "aws_caller_identity" "current" {}

# Freeze "now + teardown_after_minutes" at apply so re-plans don't drift it.
resource "time_offset" "teardown" {
  offset_minutes = var.teardown_after_minutes
}

locals {
  teardown_at = formatdate("YYYY-MM-DD'T'hh:mm:ss", time_offset.teardown.rfc3339)
}

# ── Teardown CodeBuild project ───────────────────────────────────────────────
# Source is an S3 zip of this Terraform config that the GitHub Actions apply step
# uploads (so CodeBuild destroys the exact config that created the run).
resource "aws_codebuild_project" "teardown" {
  name          = "${local.name}-teardown"
  description   = "Dead-man's-switch teardown for ${local.name}"
  service_role  = aws_iam_role.teardown.arn
  build_timeout = 60 # minutes — EKS + VPC teardown fits comfortably

  artifacts { type = "NO_ARTIFACTS" }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
    type         = "LINUX_CONTAINER"
    environment_variable {
      name  = "AWS_REGION"
      value = var.aws_region
    }
    environment_variable {
      name  = "CLUSTER_NAME"
      value = local.cluster_name
    }
    environment_variable {
      name  = "TF_STATE_BUCKET"
      value = var.warehouse_bucket
    }
  }

  source {
    type      = "S3"
    location  = "${var.warehouse_bucket}/${var.teardown_bundle_key}"
    buildspec = <<-YAML
      version: 0.2
      phases:
        install:
          commands:
            - curl -fsSL -o /tmp/tf.zip https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip
            - unzip -o /tmp/tf.zip -d /usr/local/bin
        build:
          commands:
            # Three phases. Terminating EC2 while the control plane is ALIVE just makes
            # Karpenter launch replacements (seen live 2026-08-25: 3 terminated -> 4 new
            # within minutes) — but a FULL destroy first blocks on a security group those
            # same nodes hold. So: kill the control plane, terminate, then destroy the
            # rest. Same moves as before, ordered so neither blocks the other.
            - terraform init -input=false
            # PHASE 1 — EKS only. Kills Karpenter so its nodes stop being replaced.
            # A full destroy here blocks ~13 minutes on the node security group, whose
            # ENIs belong to Karpenter instances that are NOT in Terraform state and are
            # only terminated by the step queued behind it (seen live 2026-08-27).
            - terraform destroy -target=module.eks -auto-approve -input=false || true
            # PHASE 2 — terminate leftovers now, while nothing is waiting on them.
            - |
              IDS=$(aws ec2 describe-instances --region "$AWS_REGION" \
                --filters "Name=tag:kubernetes.io/cluster/$CLUSTER_NAME,Values=owned" \
                          "Name=instance-state-name,Values=pending,running,stopping,stopped" \
                --query 'Reservations[].Instances[].InstanceId' --output text)
              if [ -n "$IDS" ]; then echo "Terminating $IDS"; aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids $IDS || true; aws ec2 wait instance-terminated --region "$AWS_REGION" --instance-ids $IDS || true; fi
            # 2b) their CNI ENIs linger 'available' and block subnet/SG delete
            - |
              for eni in $(aws ec2 describe-network-interfaces --region "$AWS_REGION" \
                --filters "Name=description,Values=aws-K8S-*" "Name=status,Values=available" \
                --query 'NetworkInterfaces[].NetworkInterfaceId' --output text); do
                aws ec2 delete-network-interface --region "$AWS_REGION" --network-interface-id "$eni" || true
              done
            # PHASE 3 — everything else; SG and VPC now delete promptly.
            - terraform destroy -auto-approve -input=false
            # 3) sweep CSI-provisioned EBS orphans (PVCs; cluster gone before delete)
            - |
              # CSI volumes carry the cluster under several different tag keys;
              # matching only kubernetes.io/cluster/<name> leaves most of them behind.
              for key in "tag:kubernetes.io/cluster/$CLUSTER_NAME" "tag:KubernetesCluster" "tag:ebs.csi.aws.com/cluster-name"; do
                case "$key" in *cluster/$CLUSTER_NAME) VAL=owned ;; *) VAL="$CLUSTER_NAME" ;; esac
                for v in $(aws ec2 describe-volumes --region "$AWS_REGION" \
                    --filters "Name=$key,Values=$VAL" "Name=status,Values=available" \
                    --query 'Volumes[].VolumeId' --output text); do
                  aws ec2 delete-volume --region "$AWS_REGION" --volume-id "$v" || true
                done
              done
            # 4) sweep EKS control-plane log groups (auto-created by EKS; a forced/partial
            #    destroy leaves them out of state → next apply fails CreateLogGroup)
            - |
              for lg in $(aws logs describe-log-groups --region "$AWS_REGION" \
                --log-group-name-prefix "/aws/eks/$CLUSTER_NAME/" \
                --query 'logGroups[].logGroupName' --output text); do
                aws logs delete-log-group --region "$AWS_REGION" --log-group-name "$lg" || true
              done
    YAML
  }

  tags = { Project = var.project_tag, RunId = var.run_id }
}

# ── One-time schedule that fires the teardown ────────────────────────────────
resource "aws_scheduler_schedule" "teardown" {
  name                         = "${local.name}-teardown"
  schedule_expression          = "at(${local.teardown_at})"
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:codebuild:startBuild"
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ ProjectName = aws_codebuild_project.teardown.name })
  }
}

# ── IAM: scheduler may start the build ───────────────────────────────────────
resource "aws_iam_role" "scheduler" {
  name = "${local.name}-teardown-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-teardown"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "codebuild:StartBuild", Resource = aws_codebuild_project.teardown.arn }]
  })
}

# ── IAM: CodeBuild teardown role (destroy the whole run) ─────────────────────
resource "aws_iam_role" "teardown" {
  name = "${local.name}-teardown"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Same broad provisioning surface as the deploy role — it must be able to delete
# everything the run created. Repo/account-scoped; tighten with a boundary later.
resource "aws_iam_role_policy" "teardown" {
  name = "destroy"
  role = aws_iam_role.teardown.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TearDown"
        Effect = "Allow"
        Action = [
          "eks:*", "ec2:*", "elasticloadbalancing:*", "autoscaling:*",
          "iam:*", "ecr:*", "logs:*", "events:*", "scheduler:*", "lambda:*",
          "codebuild:*", "sqs:*", "glue:*", "athena:*", "cloudwatch:*", "budgets:*",
          # efs.tf + the delta-logstore DynamoDB table are run resources too —
          # without these the destroy dies in refresh (AccessDenied, seen live
          # 2026-08-25: elasticfilesystem:DescribeFileSystems + dynamodb:DescribeTable).
          "elasticfilesystem:*", "dynamodb:*", "kms:*", "ssm:*"
        ]
        Resource = "*"
      },
      {
        Sid    = "StateAndBundle"
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [
          "arn:aws:s3:::${var.warehouse_bucket}", "arn:aws:s3:::${var.warehouse_bucket}/*",
          "arn:aws:s3:::${var.paimon_bucket}", "arn:aws:s3:::${var.paimon_bucket}/*"
        ]
      },
      {
        Sid      = "Lock"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
        Resource = "arn:aws:dynamodb:${var.aws_region}:${var.aws_account_id}:table/${var.project_tag}-tflock"
      }
    ]
  })
}

output "teardown_fires_at_utc" {
  value = local.teardown_at
}
