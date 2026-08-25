# EFS for Fluss remote.data.dir (ReadWriteMany, shared across coordinator/tablet/flink).
#
# WHY not S3: Fluss vends remote-log access via STS GetSessionToken, which AWS does
# NOT allow with the ASSUMED-ROLE (temporary) credentials that IRSA hands pods — so
# S3 remote.data.dir fails under IRSA the same way it failed on MinIO. A shared
# filesystem needs no token. The Paimon DATALAKE tier still lives on S3 (its FileIO
# uses the IRSA creds directly; no GetSessionToken).
resource "aws_security_group" "efs" {
  name_prefix = "${local.name}-efs-"
  vpc_id      = module.vpc.vpc_id
  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_efs_file_system" "fluss" {
  creation_token = "${local.name}-fluss"
  encrypted      = true
}

# One mount target in the (single) node AZ subnet is enough — nodes are pinned there.
resource "aws_efs_mount_target" "fluss" {
  file_system_id  = aws_efs_file_system.fluss.id
  subnet_id       = local.node_subnet_id
  security_groups = [aws_security_group.efs.id]
}

# IRSA for the EFS CSI controller.
module "efs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name             = "${local.name}-efs-csi"
  attach_efs_csi_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:efs-csi-controller-sa"]
    }
  }
}

output "efs_id" {
  value = aws_efs_file_system.fluss.id
}
