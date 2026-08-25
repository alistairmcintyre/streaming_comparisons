locals {
  # SHORT base for resource names — project_tag ("streaming-comparison", 20 chars) is
  # too long to base EKS/IAM names on: the EKS module derives an IAM name_prefix
  # "<cluster_name>-cluster-" that must be ≤ 38 chars. "sc-<run_id>" (~22) leaves room.
  # project_tag is still used for the Project tag + the standing bootstrap resources.
  name         = "sc-${var.run_id}"
  cluster_name = local.name

  # Pin the node subnet to one AZ (control plane still spans two) → no cross-AZ
  # data cost during the run. First AZ of the region.
  azs     = ["${var.aws_region}a", "${var.aws_region}b"]
  node_az = "${var.aws_region}a"
}
