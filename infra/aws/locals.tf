locals {
  # One cluster per run; the RunId keys the tags used by the kill-switch + sweep.
  name         = "${var.project_tag}-${var.run_id}"
  cluster_name = local.name

  # Pin the node subnet to one AZ (control plane still spans two) → no cross-AZ
  # data cost during the run. First AZ of the region.
  azs     = ["${var.aws_region}a", "${var.aws_region}b"]
  node_az = "${var.aws_region}a"
}
