# VPC: control plane spans 2 AZs (EKS requirement); a single NAT keeps cost down.
# Node subnets are tagged for Karpenter discovery; the Karpenter NodePool (slice 3)
# pins nodes to ONE AZ so there's no cross-AZ data cost during a run.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = local.name
  cidr = "10.0.0.0/16"

  azs             = local.azs
  private_subnets = ["10.0.0.0/19", "10.0.32.0/19"]
  public_subnets  = ["10.0.64.0/22", "10.0.68.0/22"]

  enable_nat_gateway = true
  single_nat_gateway = true # one NAT (ephemeral run; cost over HA)

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
    "karpenter.sh/discovery"          = local.cluster_name
  }
  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
}

# S3 gateway endpoint, free, and keeps Paimon/Fluss/Iceberg S3 traffic off the NAT.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
}

# Index of the single node AZ within local.azs, so we can select its private subnet.
locals {
  node_az_index  = index(local.azs, local.node_az)
  node_subnet_id = module.vpc.private_subnets[local.node_az_index]
}
