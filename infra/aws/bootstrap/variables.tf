variable "aws_region" {
  type    = string
  default = "eu-west-1"
}
variable "aws_account_id" {
  # No default: the account is supplied per deployment, from the
  # AWS_ACCOUNT_ID repository variable in CI (TF_VAR_aws_account_id).
  type = string
}
# REQUIRED, scopes the OIDC trust, e.g. "alistairmc/streaming_comparisons".
variable "github_repo" {
  type = string
}
variable "github_subjects" {
  type    = list(string)
  default = ["ref:refs/heads/*"]
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
variable "ecr_repos" {
  type = list(string)
  default = [
    "fluss-server", "fluss-flink", "generator", "kafka-connect",
    "spark-iceberg", "spark-delta", "spark-hudi", "flink-paimon", "latency-exporter"
  ]
}
