# k8s workloads (applied by GitHub Actions after `terraform apply`)

Manifests are **templates** with `${VAR}` placeholders rendered by the workflow
via `envsubst` from Terraform outputs + `env/aws.env` (same pattern as the Fluss
SQL). Values injected: `CLUSTER_NAME`, `AWS_REGION`, `ECR_REGISTRY`, `WORKLOAD_ROLE_ARN`,
`KARPENTER_NODE_ROLE`, `PAIMON_BUCKET`, `WAREHOUSE_BUCKET`, image tags.

## Operators (Helm — installed once per run, before the CRs)

```bash
# Karpenter (uses the IAM role + SQS from the Terraform karpenter module outputs)
helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter -n kube-system \
  --set "settings.clusterName=${CLUSTER_NAME}" \
  --set "settings.interruptionQueue=${KARPENTER_QUEUE}" \
  --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${KARPENTER_CONTROLLER_ROLE}"
# Strimzi (Kafka), Flink Kubernetes Operator, cert-manager (Flink op dep)
helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator -n kafka --create-namespace
helm upgrade --install cert-manager jetstack/cert-manager -n cert-manager --create-namespace --set crds.enabled=true
helm upgrade --install flink-operator \
  oci://ghcr.io/apache/flink-kubernetes-operator -n flink --create-namespace
# Spark Operator (SparkApplications), and Prometheus + Grafana (observability)
helm upgrade --install spark-operator spark-operator/spark-operator -n spark --create-namespace
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  --set grafana.dashboardProviders... # or import infra/aws/k8s/grafana/pipeline-comparison-live.json
```

## Latency emit contract (what feeds the live p50/p95/p99)

Each pipeline emits **sampled** (~1/100) per-record timings to the `pipeline_latency`
Kafka topic; the **latency-exporter** (`docker/latency-exporter`, deployed by
`94-observability.yaml`) turns them into the `processing_delay_seconds` Prometheus
histogram + S3 Parquet. Event JSON:

```json
{"pipeline": "fluss|flink-paimon|spark-iceberg|spark-delta",
 "executed_at_ms": 1750000000000, "ingest_ts_ms": 1750000000123}
```

- **Flink SQL** pipelines: add a Kafka sink table and an `INSERT ... SELECT pipeline,
  UNIX_TIMESTAMP(executed_at)*1000, UNIX_TIMESTAMP()*1000 ... WHERE RAND() < 0.01`
  off the bronze/silver stream.
- **Spark** pipelines: a `foreachBatch` that samples the micro-batch and produces the
  same JSON to Kafka.
This per-pipeline emit is the remaining wiring (add during first-run iteration);
until it's in, the dashboard still shows infra metrics (throughput/lag) from
Prometheus.

## Apply order (`kubectl apply` the rendered manifests)

```
00-namespaces.yaml   # namespaces + IRSA-annotated service accounts
10-karpenter.yaml    # NodePool + EC2NodeClass (single-AZ, spot→on-demand)
20-storage.yaml      # gp3 StorageClass
30-kafka.yaml        # Strimzi Kafka (KRaft)
40-postgres.yaml     # source DB + init DDL
50-debezium.yaml     # KafkaConnect + KafkaConnector (trades/accounts publications)
60-fluss.yaml        # Fluss coordinator + tablet (remote.data.dir on S3 — STS works on AWS)
70-flink-fluss.yaml  # FlinkDeployment session + submitter Job (create/bronze/gold/tiering)
80-generator.yaml    # trades generator → Postgres
```

Slice 3b adds the Spark iceberg/delta/paimon stacks + maintenance CronJobs; slice 4
adds kube-prometheus-stack + the latency-exporter + the "Pipeline Comparison — Live"
dashboard. Reach the Flink UI / Grafana with `kubectl port-forward` (no LoadBalancer
→ no orphan ELB at teardown).
