variable "aws_region" {
  type    = string
  default = "eu-west-1"
}
variable "aws_account_id" {
  # No default: the account is supplied per deployment, from the
  # AWS_ACCOUNT_ID repository variable in CI (TF_VAR_aws_account_id).
  type = string
}
variable "project_tag" {
  type    = string
  default = "streaming-comparison"
}
variable "paimon_bucket" {
  type    = string
  default = "streaming-comparison-amc-paimon"
}
variable "warehouse_bucket" {
  type    = string
  default = "streaming-comparison-amc-warehouse"
}

# Unique per run (timestamp), tags every resource so the kill-switch + orphan
# sweep can find and destroy exactly this run. Passed in by the workflow.
variable "run_id" {
  type    = string
  default = "manual"
}

variable "cluster_version" {
  type    = string
  default = "1.34"
}

# Namespaces whose service accounts may assume the S3/Glue/Athena workload role.
variable "workload_namespaces" {
  type    = list(string)
  default = ["streaming", "kafka", "flink", "spark", "fluss"]
}

# Hard cutoff: minutes from apply until the EventBridge dead-man's switch fires.
variable "teardown_after_minutes" {
  type    = number
  default = 150 # 2.5 h
}

# S3 key (in warehouse_bucket) of the zipped Terraform config the workflow uploads
# so the teardown CodeBuild destroys the exact config that created the run.
variable "teardown_bundle_key" {
  type    = string
  default = "teardown/eks-run.zip"
}
