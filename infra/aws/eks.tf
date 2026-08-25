# EKS + IRSA (OIDC) + a small system node group that hosts CoreDNS / the EBS CSI
# controller / the Karpenter controller. Karpenter then provisions the workload
# nodes (spot→on-demand) pinned to one AZ (NodePool in slice 3).
#
# NOTE: pinned to EKS module v20 / VPC v5 arg names — run `terraform plan` and
# reconcile any arg drift before the first apply.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.cluster_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # OIDC provider for IRSA is created by the module (enable_irsa defaults true).
  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = {}
    aws-ebs-csi-driver     = { service_account_role_arn = module.ebs_csi_irsa.iam_role_arn }
    aws-efs-csi-driver     = { service_account_role_arn = module.efs_csi_irsa.iam_role_arn }
    eks-pod-identity-agent = {}
  }

  # System node group — 2 on-demand for control/system pods + Karpenter controller.
  # Pinned to one AZ (single-AZ node placement).
  eks_managed_node_groups = {
    system = {
      instance_types = ["m6i.large"]
      ami_type       = "AL2023_x86_64_STANDARD"
      min_size       = 2
      max_size       = 3
      desired_size   = 2
      subnet_ids     = [local.node_subnet_id]
      labels         = { role = "system" }
    }
  }

  # Let the GitHub deploy role administer the cluster (kubectl deploys).
  access_entries = {
    gha_deploy = {
      principal_arn = data.aws_iam_role.github_deploy.arn
      policy_associations = {
        admin = {
          policy_arn   = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
  }

  # Nodes must be discoverable + drainable by Karpenter.
  node_security_group_tags = { "karpenter.sh/discovery" = local.cluster_name }
}

# IRSA role for the EBS CSI driver (needed for PVCs: Kafka/Fluss/Postgres storage).
module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name             = "${local.name}-ebs-csi"
  attach_ebs_csi_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}

output "cluster_name" {
  value = module.eks.cluster_name
}
output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}
output "oidc_provider_arn" {
  value = module.eks.oidc_provider_arn
}
