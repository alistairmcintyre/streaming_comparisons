# Karpenter IAM + interruption SQS (the controller Helm release + the NodePool /
# EC2NodeClass CRs are applied to the cluster in slice 3 / GitHub Actions, using
# the outputs below). NodePool pins nodes to ${local.node_az}, capacity-type
# spot with on-demand fallback.
module "karpenter" {
  source  = "terraform-aws-modules/eks/aws//modules/karpenter"
  version = "~> 20.24"

  cluster_name          = module.eks.cluster_name
  enable_v1_permissions = true
  namespace             = "kube-system"

  node_iam_role_use_name_prefix = false
  node_iam_role_name            = "${local.name}-karpenter-node"

  # Pod Identity for the controller (simpler than IRSA for Karpenter v1).
  create_pod_identity_association = true

  # The interruption controller needs ec2:DescribeInstanceStatus, and the upstream
  # module's policy omits it. Karpenter logs this at startup and then carries on with
  # interruption handling dead:
  #   ERROR ... ec2:DescribeInstanceStatus permission is not allowed, update the IAM
  #   policy and restart the Karpenter deployment
  # Harmless while every node is on-demand, which is what a live run happened to get,
  # but it means a spot reclaim would go unhandled: no drain, no cordon, just an
  # instance disappearing under whatever was running on it.
  iam_policy_statements = [
    {
      effect    = "Allow"
      actions   = ["ec2:DescribeInstanceStatus"]
      resources = ["*"]
    }
  ]
}

output "karpenter_controller_role_arn" {
  value = module.karpenter.iam_role_arn
}
output "karpenter_node_role_name" {
  value = module.karpenter.node_iam_role_name
}
output "karpenter_queue_name" {
  value = module.karpenter.queue_name
}
