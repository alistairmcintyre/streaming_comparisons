# AWS / EKS deployment — runbook & decisions

Ephemeral, cost-capped EKS runs of the streaming comparison (Flink · Fluss ·
StarRocks · Paimon/Iceberg) on `eu-west-1`. Each run is created, exercised for
~2 h, and **hard-torn-down at 2.5 h** — nothing is left standing.

## Fixed inputs

| | |
|---|---|
| Account | `167217327348` |
| Region | `eu-west-1` |
| Buckets | `streaming-comparison-amc-paimon` (Paimon/Fluss lake), `streaming-comparison-amc-warehouse` (Iceberg warehouse + Athena results) |
| Catalog | AWS Glue (Iceberg tables), Athena for ad-hoc |

## Decisions (locked)

- **IaC:** Terraform (`infra/aws/`), `apply` on run start, `destroy` on end.
- **Nodes:** Karpenter, **spot with on-demand fallback**, **pinned to one AZ**
  (control plane spans 2 AZs; node pool single-AZ → zero cross-AZ data cost).
- **Identity — two OIDC trusts, no static keys:**
  1. **GitHub → AWS** deploy role (trust scoped to this repo) for CI apply/deploy/destroy.
  2. **EKS IRSA** workload role scoped to the two buckets + Glue + Athena; the pods
     that touch S3 (Fluss, Flink, StarRocks) assume it via SA annotation. (The Fluss
     server entrypoint already drops blank S3 keys → IAM credential chain.)
- **Orchestration + STRICT 2.5 h cutoff:** GitHub Actions drives
  `apply → build/push images → deploy → run (1k then 10k) → snapshot p50/p95 → destroy`,
  **backed by a Terraform-created one-time EventBridge schedule at now+2.5 h → teardown
  Lambda** (dead-man's switch: fires even if the workflow dies). Everything tagged
  `Project=streaming-comparison RunId=<ts>`; an orphan sweep removes lingering
  ELBs / EBS / ENIs. AWS Budget alert as a backstop.
- **CDC:** keep Postgres + Debezium; test **1k/s then 10k/s** first. (30k/s later would
  shard publications or use generator→Kafka-direct, since one Debezium slot is the
  ceiling — the limit is per replication slot, combined across the publication's tables.)
- **Serving/OLAP:** **deferred** — StarRocks is a later phase (serving layer + heavy
  analytical-query benchmark). Not needed for the streaming-pipeline comparison; it can
  read the lake via a Glue external catalog whenever added. Keeping scope lean now.
- **Correctness (no extra infra):** a workflow step reconciles each pipeline against the
  source — (1) completeness: distinct `trade_id` in each pipeline's silver == distinct
  trades from source (Kafka `app.public.trades` count); (2) fold consistency (existing
  `scripts/integration/`): Σ signed qty == gold `net_quantity`, Σ `trade_count` == source.
  Run via Athena/Spark; result → `s3://…-warehouse/benchmarks/<RunId>/`.

## Observability — Prometheus + Grafana (StarRocks NOT in the metrics path)

`kube-prometheus-stack` on-cluster (~$0 extra). Metrics are a **continuous time
series over the whole run** — not a single snapshot — so a pipeline that degrades
late in the 2 h is visible. Grafana reads **Prometheus** (never Kafka directly).

**Processing-delay pipeline (uniform, engine-agnostic):**
```
each pipeline → sampled (pipeline, executed_at, ingest_ts) → Kafka topic pipeline_latency
   → ONE latency-exporter consumer:
        ├─ Prometheus histogram  processing_delay_seconds{pipeline=...}
        └─ append raw events → s3://…-warehouse/benchmarks/<RunId>/latency/*.parquet
```
- `ingest_ts` = processing wall-clock stamped on the **per-record** (bronze/silver)
  write; `delay = ingest_ts − executed_at`. Sample ~1/100 records so emit overhead is
  nil at 10k–30k/s.
- **Grafana "Pipeline Comparison — Live"**, one line per pipeline, refresh 5–15 s:
  - Throughput (rec/s) per pipeline — instant liveness (flat-zero = inactive).
  - Kafka consumer lag, Flink watermark/event-time lag, backpressure per pipeline.
  - **p50/p95/p99 processing delay OVER TIME** via
    `histogram_quantile(0.95, sum(rate(processing_delay_seconds_bucket[1m])) by (le, pipeline))`.
  - View via `kubectl port-forward` (no LoadBalancer → no orphan ELB).
- **Durable, exact analysis:** the raw events land in S3 as Parquet (survive teardown);
  compute exact percentiles / any window / rate-of-change with Athena or DuckDB after
  the run. (Prometheus quantiles are bucket-approximate + its TSDB dies at teardown, so
  S3 is the system of record.) No fixed "1-hour snapshot" — the whole curve is kept.
- **StarRocks is decoupled from metrics.** It stays only as the SERVING/reporting layer
  (gold-table + heavy analytical queries, prod parity). A pure latency-benchmark run can
  omit StarRocks entirely.
- Managed alternative (not chosen): AMP ~$1–2/run + AMG ~$9/mo/editor ≈ $20–30/mo.

## Maintenance jobs (a benchmark variable, not an afterthought)

Under continuous streaming writes the formats need housekeeping, and its cadence is
a major differentiator — small-file accumulation is exactly what drives the
degrade-over-time you see on the dashboard:
- **Delta:** compaction is **IN-pipeline** — Optimized Writes + Auto Compaction
  (`delta.autoOptimize.optimizeWrite`/`.autoCompact` on the tables in
  ddl/create_tables_delta.py, plus session confs on the streaming write). The only
  separate job is **VACUUM** (GC; no auto-vacuum in OSS) — jobs/spark-delta/maintenance_vacuum.py.
  Single committer (the streaming writer), so the DynamoDB LogStore is optional here.
- **Iceberg:** no in-writer auto-compaction in Spark structured streaming, so
  `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` run as a separate
  Spark `CALL` job (jobs/spark-iceberg/maintenance_compaction.py).
- **Paimon:** self-compacts (`compaction.optimization-interval`, `full-compaction.delta-commits`)
  — no separate job; Iceberg-view freshness is full-compaction-gated.
Run each at a realistic cadence for a fair steady-state; the time series then shows
each format's compaction sawtooth + baseline drift over the run.

## Two Terraform states: `bootstrap/` (standing) + `.` (per-run)

`infra/aws/bootstrap/` is applied ONCE and never torn down — GitHub OIDC provider +
deploy role, the `streaming-comparison-tflock` table, and ECR repos (images persist
across runs). If the account already has a GitHub OIDC provider, import it first.
```bash
cd infra/aws/bootstrap && terraform init && \
  terraform apply -var 'github_repo=<owner>/<repo>'
```
`infra/aws/` (the run) is created/destroyed per run and references the standing
resources by name/data source. Everything below is per-run.

## What the per-run Terraform provisions (`infra/aws/`)

1. GitHub OIDC provider + deploy role (repo-scoped trust).
2. VPC (2-AZ control-plane subnets) + single-AZ private node subnet + **S3 gateway
   endpoint** (free; keeps S3 off NAT) + ECR/STS interface endpoints.
3. EKS + OIDC/IRSA + Karpenter (spot→on-demand, single-AZ) + EBS CSI.
4. ECR repos (fluss-server, fluss-flink, generator) — CI builds/pushes.
5. IRSA workload role: `s3:*` on the two buckets, Glue + Athena on the run's DB/workgroup.
6. EventBridge one-time (now+2.5 h) → teardown Lambda + `RunId` tags + orphan sweep + Budget.

## Run lifecycle (GitHub Actions, OIDC)

```
apply (≤30m) → build+push images → deploy operators+workloads → wait-ready
      → start pipelines (1k, then 10k) → live Grafana → ~1h snapshot p50/p95 → destroy
```
Hard stop at 2.5 h via the EventBridge kill switch regardless of workflow state.

## Cost per run (eu-west-1, approx)

~$3–7 on-demand / ~$1.5–3 spot compute + <$1 S3 + ~$0.4 control-plane (per-run hours).
At 2–3 runs/week ≈ **$25–50/mo**, no standing baseline.
