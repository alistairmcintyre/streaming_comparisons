# Strict 2.5h cutoff, an AWS-side dead-man's switch that does not depend on the
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
  build_timeout = 60 # minutes. EKS + VPC teardown fits comfortably

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
    # aws_account_id has no default (the account is never hardcoded in this repo), so
    # every terraform invocation must be handed one. This CodeBuild project is the
    # dead-man's switch: it runs `terraform destroy` with no workflow and no env/aws.env
    # around it, so without this the 2.5h teardown would fail on "No value for required
    # variable" and a run could bill until someone noticed by hand.
    environment_variable {
      name  = "TF_VAR_aws_account_id"
      value = var.aws_account_id
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
            # 1.10+ is required: the backend uses use_lockfile (S3 native
            # locking) and 1.9.8 does not understand it, so init would fail here and
            # the dead-man's switch would quietly stop being able to destroy anything.
            - curl -fsSL -o /tmp/tf.zip https://releases.hashicorp.com/terraform/1.10.5/terraform_1.10.5_linux_amd64.zip
            - unzip -o /tmp/tf.zip -d /usr/local/bin
        build:
          commands:
            # Three phases. Terminating EC2 while the control plane is ALIVE just makes
            # Karpenter launch replacements (seen live 2026-08-25: 3 terminated -> 4 new
            # within minutes), but a FULL destroy first blocks on a security group those
            # same nodes hold. So: kill the control plane, terminate, then destroy the
            # rest. Same moves as before, ordered so neither blocks the other.
            # -reconfigure: backend moved from dynamodb_table to use_lockfile; without it
            # init fails with "Backend configuration changed" under -input=false.
            - terraform init -input=false -reconfigure
            # PHASE 1. EKS only. Kills Karpenter so its nodes stop being replaced.
            # A full destroy here blocks ~13 minutes on the node security group, whose
            # ENIs belong to Karpenter instances that are not in Terraform state and are
            # only terminated by the step queued behind it (seen live 2026-08-27).
            - terraform destroy -target=module.eks -auto-approve -input=false || true
            # PHASE 2, terminate leftovers now, while nothing is waiting on them.
            - |
              IDS=$(aws ec2 describe-instances --region "$AWS_REGION" \
                --filters "Name=tag:kubernetes.io/cluster/$CLUSTER_NAME,Values=owned" \
                          "Name=instance-state-name,Values=pending,running,stopping,stopped" \
                --query 'Reservations[].Instances[].InstanceId' --output text)
              if [ -n "$IDS" ]; then echo "Terminating $IDS"; aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids $IDS || true; aws ec2 wait instance-terminated --region "$AWS_REGION" --instance-ids $IDS || true; fi
            # 2b) EFS mount targets hold an ENI in the node subnet and the EFS security
            #     group has an ingress rule referencing the NODE security group, so
            #     neither the subnet nor that SG can go until the mount target does.
            #     Deletion is asynchronous, so start it before the sweep.
            - |
              for fs in $(aws efs describe-file-systems --region "$AWS_REGION" \
                  --query "FileSystems[?contains(CreationToken, '$CLUSTER_NAME')].FileSystemId" --output text); do
                for mt in $(aws efs describe-mount-targets --region "$AWS_REGION" --file-system-id "$fs" \
                    --query 'MountTargets[].MountTargetId' --output text); do
                  echo "deleting EFS mount target $mt"
                  aws efs delete-mount-target --region "$AWS_REGION" --mount-target-id "$mt" || true
                done
              done
            # 2c) CNI ENIs linger 'available' and block subnet/SG delete. POLL: `wait
            #     instance-terminated` returns when the INSTANCE is gone, but its ENIs take
            #     another minute or two to detach, so a single sweep finds nothing.
            - |
              for _ in $(seq 1 6); do
                for eni in $(aws ec2 describe-network-interfaces --region "$AWS_REGION" \
                  --filters "Name=description,Values=aws-K8S-*" "Name=status,Values=available" \
                  --query 'NetworkInterfaces[].NetworkInterfaceId' --output text); do
                  aws ec2 delete-network-interface --region "$AWS_REGION" --network-interface-id "$eni" || true
                done
                REMAINING=$(aws ec2 describe-network-interfaces --region "$AWS_REGION" \
                  --filters "Name=description,Values=aws-K8S-*" \
                  --query 'NetworkInterfaces[].NetworkInterfaceId' --output text)
                [ -z "$REMAINING" ] && break
                sleep 30
              done
            # 2d) THE ONE THAT ACTUALLY FAILED. This kill switch died with
            #       DependencyViolation: resource sg-... has a dependent object
            #     leaving a VPC, subnets, an IGW, an EIP and 190GB of EBS behind. A
            #     security group has exactly two kinds of dependent object: ENIs that use
            #     it, and RULES IN OTHER GROUPS that reference it. efs.tf creates the
            #     second kind, and nothing here cleared it, the GHA destroy path gained
            #     this fix and the BACKSTOP did not, which is precisely backwards.
            - |
              for SG in $(aws ec2 describe-security-groups --region "$AWS_REGION" \
                  --filters "Name=tag:kubernetes.io/cluster/$CLUSTER_NAME,Values=owned" \
                  --query 'SecurityGroups[].GroupId' --output text); do
                for eni in $(aws ec2 describe-network-interfaces --region "$AWS_REGION" \
                    --filters "Name=group-id,Values=$SG" "Name=status,Values=available" \
                    --query 'NetworkInterfaces[].NetworkInterfaceId' --output text); do
                  aws ec2 delete-network-interface --region "$AWS_REGION" --network-interface-id "$eni" || true
                done
                for OTHER in $(aws ec2 describe-security-groups --region "$AWS_REGION" \
                    --filters "Name=ip-permission.group-id,Values=$SG" \
                    --query "SecurityGroups[?GroupId!='$SG'].GroupId" --output text); do
                  PERMS=$(aws ec2 describe-security-groups --region "$AWS_REGION" --group-ids "$OTHER" \
                    --query "SecurityGroups[0].IpPermissions[?UserIdGroupPairs[?GroupId=='$SG']]" --output json)
                  if [ -n "$PERMS" ] && [ "$PERMS" != "[]" ]; then
                    echo "revoking $OTHER ingress that references $SG"
                    aws ec2 revoke-security-group-ingress --region "$AWS_REGION" \
                      --group-id "$OTHER" --ip-permissions "$PERMS" || true
                  fi
                done
              done
            # PHASE 3, everything else; SG and VPC now delete promptly.
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

# Same broad provisioning surface as the deploy role, it must be able to delete
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
          # efs.tf + the delta-logstore DynamoDB table are run resources too, 
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
      }
      # No DynamoDB "Lock" statement any more: the backend uses use_lockfile, so the lock
      # is a .tflock object next to the state and the S3 grant above already covers it.
      # (The TearDown statement's dynamodb:* still covers the Delta log-store table.)
    ]
  })
}

output "teardown_fires_at_utc" {
  value = local.teardown_at
}
