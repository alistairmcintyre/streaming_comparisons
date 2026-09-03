# Streaming lakehouse comparison

Five streaming pipelines over one CDC source, built to the same medallion shape and
measured against each other on cost-capped, ephemeral AWS infrastructure.

| Track | Engine | Table format | Gold maintained by |
|---|---|---|---|
| `spark-delta` | Spark Structured Streaming | Delta Lake | Change Data Feed, incremental MERGE |
| `spark-iceberg` | Spark Structured Streaming | Iceberg | batch fold (see [Finding 1](#finding-1-an-iceberg-upsert-silver-cannot-be-stream-read)) |
| `spark-hudi` | Spark Structured Streaming | Hudi MoR | incremental query |
| `flink-paimon` | Flink SQL | Paimon | native retraction, streaming `GROUP BY` |
| `flink-fluss` | Flink SQL | Fluss to Paimon to Iceberg | native retraction over the Fluss changelog |

The question behind the project is narrow and practical: if silver holds mutable
state and gold has to stay current, which combinations of engine and table format
actually support that, and what does each one cost you.

## Source and shape

```
Postgres  ->  Debezium  ->  Kafka  ->  bronze  ->  silver  ->  gold
                                       (raw CDC)   (cleaned,   (net open
                                                    deduped)    positions)
```

Two streams. `trades` are immutable executions, so `silver.trades` is insert-only
and deduped on `trade_id`. `accounts` are mutable, so `silver.accounts` is an SCD2
dimension with validity ranges and one current row per account. Gold is net open
position per `(account_id, symbol)`, which is a current-state metric and therefore
the interesting case.

Fluss has no bronze layer. Its primary-key table is already the cleaned landing
table, so it runs one hop fewer, and the results record that rather than inventing a
bronze to make the table count match.

## What it found

### Finding 1: an Iceberg upsert silver cannot be stream-read

A Flink upsert sink (`upsert-enabled=true`, required for a one-row-per-key current
view) writes an equality delete plus a data record for every row, inserts included,
because a streaming write cannot know whether the key already exists. Any commit
containing delete files is an Iceberg `RowDelta`, whose operation is `overwrite`,
never `append`. Flink's Iceberg streaming source emits no changelog from overwrite
snapshots, so a downstream gold job reads nothing. Verified with gold empty after
300s while silver was fully populated and batch-readable. The only `append` snapshots
on that table are empty checkpoint heartbeats.

Spark's `MERGE INTO` on Iceberg produces the same shape. So under Iceberg you pick
one of: an append-only changelog silver with the current view derived on read, or a
current-view silver with a batch gold. This project takes the second for the Iceberg
track, which is why `spark-iceberg` is the one pipeline whose gold is a batch fold.

This is an engine and format limitation, not an environment one. It reproduces
identically on a laptop and on EKS.

### Finding 2: the streaming-native formats do all three

Paimon, Delta and Hudi each give a current-view table that is also readable as a
changelog, through `changelog-producer`, Change Data Feed and incremental query
respectively. That combination, stream write plus current value per key plus stream
read, is what makes an incremental gold possible at all.

### Finding 3: the failures worth writing down were operational, not conceptual

The pipeline logic was rarely the problem. What broke runs was version skew, image
contents, catalog registration, and Kubernetes autoscaling behaviour. A sample, all
found by measurement rather than inspection:

- Hudi's column-stats data skipping expands `key IN (<one literal per key>)` into a
  nested binary predicate, and Catalyst resolves expressions recursively, so a batch
  carrying enough distinct keys overflows the driver stack. Depth equals the number
  of keys, so it is invisible in a four-key test and certain in production, where a
  restart backlog fills the batch with the whole key space.
- Athena reads an Iceberg table's columns from the Glue definition, not from the
  metadata file the pointer names. Tables registered with an empty column list answer
  `SELECT count(*)` and fail `SELECT *`.
- Delta tables with deletion vectors cannot be registered through Athena DDL, though
  the query engine reads them. They register through the Glue API instead.
- Karpenter consolidation targets underutilized nodes, which describes a benchmark
  node for most of its life. With every Spark app running a single executor, a
  consolidation event is not a degraded query but a dead one.

## Design principles

Written from this project's findings, but format-agnostic.

**1. Never full-scan silver to build gold.** A gold aggregation that re-reads the
whole silver table every micro-batch is `O(rows)` per trigger. At millions of rows
it is wasteful and at billions it is dead. Neither clustering nor bigger compute
fixes it: clustering speeds up *filtered* reads, and a global aggregation has no
predicate to skip on. Gold must be maintained incrementally from the change stream,
so that work is proportional to the number of changes rather than the table size.

**2. Ask first whether the metric depends on a mutable field.** This one decision
drives the whole design.

| Metric | Example | Approach | Needs a current-view silver? |
|---|---|---|---|
| Immutable event | trade count, notional volume, VWAP | aggregate the append stream with event-time windows and a watermark | No, read the append log directly |
| Current state | open exposure, net position, count by current status | retraction-aware incremental view maintenance over the changelog | Yes |

Much of analytics is the first case, where updates are irrelevant to the measure.
Only reach for the harder pattern when the metric genuinely tracks mutable state.

**3. Apply signed deltas, do not recount.** Consume the changes and fold them into a
small running-aggregate table, which is itself the state: insert and update-postimage
give `+1`, delete and update-preimage give `-1`. A group-key change arrives as both,
so it nets correctly with no recount. For sums, the delta is `post - pre`.

**4. Match the format to the update ratio.** LSM formats (Paimon, Hudi MoR) absorb
high update rates best, appending deltas and compacting in the background. Delta's
MERGE rewrites files, or uses deletion vectors, which costs more write amplification
at extreme update rates. Separately, Flink does retraction-aware aggregation natively
with bounded per-key state, whereas Spark has no equivalent and needs a hand-rolled
changelog MERGE.

**5. Make replays harmless.** `foreachBatch` is at-least-once, so running counters
double-count on retry. Stamp each write with an app id and the batch id (Delta's
`txnAppId` and `txnVersion`) so a replayed batch is a no-op. Flink's checkpointed
state gives this for free. Order last-writer-wins by a sequence field that survives
deletes, such as Debezium's `ts_ms`, and never by an `updated_at` column, which is
null on a delete.

**6. Partition and cluster for the read side only.** These skip data for predicates.
Cluster by the query predicate, not the streaming aggregation key, and never
partition silver by a mutable field, since an update then moves the row between
partitions.

**7. Compaction is not garbage collection.** In-writer compaction avoids
concurrent-writer conflicts, but every format still needs a separate reclaim step
(`VACUUM`, `expire_snapshots`, Hudi cleaning, Paimon snapshot expiry). Compaction
increases the number of dead files, so it makes GC more necessary, not less.

## Format comparison

Architecture-derived relative ratings, not benchmarks. Use them for design direction
and for the reasoning in the last column, not for capacity planning.

| Format | Core design | Update model |
|---|---|---|
| Iceberg | snapshot and manifest tree over immutable files | copy-on-write, or MoR via delete files |
| Delta | transaction log over data files, plus deletion vectors | CoW MERGE, or DVs for MoR-style updates |
| Hudi | timeline plus a record-level index; CoW or MoR | index locates the file for a key |
| Paimon | LSM tree on the lake | append to L0, background compaction merges |

| Dimension | Iceberg | Delta | Hudi | Paimon | Why |
|---|:---:|:---:|:---:|:---:|---|
| Upsert write, high update rate | ★★ | ★★★ | ★★★★ | ★★★★★ | Iceberg CoW rewrites whole files; Paimon appends to L0 |
| Streaming changelog (silver to gold) | ★★ | ★★★★ | ★★★★ | ★★★★★ | Paimon native; Delta CDF and Hudi incremental first-class; Iceberg upsert not stream-readable |
| Large analytical scan | ★★★★★ | ★★★★ | ★★★ | ★★★ | Iceberg manifest and column-stats pruning is best; MoR tables pay a merge cost |
| Point lookup by key | ★★★ | ★★★ | ★★★★ | ★★★★ | Hudi record index, Paimon LSM key; Iceberg and Delta scan and filter |
| Partitioning | ★★★★★ | ★★★★ | ★★★ | ★★★★ | Iceberg hidden partitioning and partition evolution |
| Managed maintenance | ★★★ | ★★★★ | ★★★★★ | ★★★★★ | Hudi and Paimon run compaction as table services; Iceberg is explicit jobs only |
| Engine breadth | ★★★★★ | ★★★ | ★★★★ | ★★★ | Iceberg broadest; Delta Spark-first; Paimon Flink-first |

Picking by workload: high-update streaming CDC with a current-view gold suits Paimon
with Flink, or Hudi MoR; an append-heavy analytical lake with Athena or Trino
consumers suits Iceberg; a Spark-centric shop wanting one format for batch and
streaming suits Delta.

## Configuration

One value needs setting before anything touches AWS, and it lives in one place.

```bash
cp env/aws.example.env env/aws.env      # gitignored
# set AWS_ACCOUNT_ID in it
```

`scripts/ecr-env.sh` is the only thing that reads it, deriving the container registry
that the Docker-backed checks pull from. Nothing in the repo hardcodes an account id,
and `tests/check_no_account_id.py` fails the suite if one reappears.

CI cannot read a gitignored file, so GitHub Actions takes the same value from the
repository **secret** `AWS_ACCOUNT_ID` (Settings, then Secrets and variables, then
Actions, then Secrets). That feeds the workflow's `ACCOUNT`, the deploy role ARN and
`TF_VAR_aws_account_id`, so Terraform picks it up on every plan, apply and destroy
without a `-var` on each call.

A secret rather than a variable, because this repository is public and that makes its
Actions logs public too. The account id is echoed as part of the ECR registry hostname
on every `docker push`, so a variable would republish it on every run. GitHub masks a
secret's value wherever it appears in a log, including inside that hostname.

The offline checks need none of this. `make test` runs on a fresh clone with no AWS
configuration at all; `make test-all` reports what is missing and skips the Docker
checks rather than failing.

`env/local.env` holds the local Docker Compose settings and needs no edits.

## Running locally

Docker Compose brings up Postgres, Kafka, Debezium, MinIO and the pipelines.

```bash
make all                # infra, tables, Debezium connectors, generators, Spark tracks
make status             # connector and container state
make logs-delta         # also logs-spark, logs-flink-paimon, logs-connect
make down               # or `make clean` to drop volumes too
```

`make all` composes the individual steps, which can also be run on their own:
`up`, `create-tables`, `register-connectors`, `start-generators`, `start-spark`,
`start-delta`, `start-flink-paimon`, `start-compactor`. `make sql` opens a SQL shell
and `make consistency-check` reconciles the pipelines against the source.

Fluss runs from a separate compose file because it needs source-built artifacts:

```bash
FLUSS_SRC=~/git/apache/fluss docker/fluss/build-fluss.sh
docker compose -f docker-compose.fluss.yml up -d
```

The source build is required rather than convenient. On the 0.9.1 release, Fluss
appends system columns to the lake table and `__timestamp` is `TIMESTAMP_LTZ(3)`,
which Paimon's Iceberg compatibility rejects because it only supports precision 4 to
9. No Iceberg metadata is written at all and the table is invisible to Athena. FIP-27
gives new lake tables a clean schema without those columns, and it is on `main` but
not in any release.

Four other things about this stack are non-obvious:

- The images are built, not pulled. The base `apache/fluss` image ships
  paimon-bundle but not paimon-s3, so the datalake warehouse cannot initialise.
- `remote.data.dir` is a shared local volume, not S3. Fluss vends S3 access to
  clients through STS `GetSessionToken`, which MinIO does not implement, so the
  client reaches real AWS STS and gets a 403. That blocks the read path while writes
  still succeed. The Paimon datalake tier does live on S3, written by the tiering job.
- Rebuild all four images together. A stale image against a fresh one gives
  `InvalidClassException: serialVersionUID`.
- The TaskManager needs `taskmanager.memory.process.size: 4096m`. The quickstart
  default OOMs running bronze, gold and tiering together, which kills the cluster.

### Local stack, lake on real S3 and Glue

The tier between a laptop and a cluster. Kafka, Postgres, Debezium and the generators
stay in Compose; only the lake moves to real S3 and the real Glue catalog.

```bash
cp env/aws.example.env env/aws.env         # set AWS_ACCOUNT_ID
aws sso login --profile streaming-comparisons
. scripts/lake-aws-env.sh && make up start-delta
```

It writes under `s3://<warehouse>/_devlake/<you>/`, a per-user prefix that no benchmark
run reads, and the script prints the cleanup command.

Worth using because most of what has actually broken on this project broke against real
AWS rather than under MinIO, and none of it needed a cluster to find: Glue registering
tables with an empty column list, Athena refusing Delta tables with deletion vectors,
Hudi's timeline wedging on an incomplete instant, and the partition-column ordering that
made Athena misread Hudi. All reproducible here in minutes.

What it cannot tell you, so a clean run here is not a green light for a full run:

- **IRSA credential behaviour.** `WebIdentityTokenCredentialsProvider` only exists on a
  pod with a projected service-account token, so the failure that killed the Hudi jobs
  on a live run cannot occur here at all.
- **Node disruption**, multi-executor distribution, and sustained 1000/s throughput.

Everything else has a cheaper tier still: `make test` needs no AWS whatever, and
`make test-all` runs SCD2 and gold folds against real Delta, Iceberg and Hudi tables
locally.

## Running on AWS

Each run creates an EKS cluster, exercises it, snapshots results to S3 and destroys
everything. A Terraform-created EventBridge schedule fires a teardown Lambda at
2.5 hours regardless of what the workflow does, so a hung run cannot leak cost.

Triggered by `workflow_dispatch` on `.github/workflows/eks-run.yml`, whose inputs
cover generator rate, run length, teardown mode and CDC wire format.

Decisions worth knowing before running it:

- Nodes come from Karpenter, spot with on-demand fallback, pinned to one AZ so
  there is no cross-AZ data cost. Consolidation is `WhenEmpty`, because repacking
  nodes mid-run kills executors and changes the hardware under the comparison.
- No static credentials anywhere. GitHub authenticates to AWS through OIDC with a
  role scoped to this repo; pods reach S3, Glue and Athena through EKS IRSA.
- Everything is tagged `Project=streaming-comparison RunId=<ts>`, and an orphan
  sweep removes lingering ELBs, EBS volumes and ENIs.

Results land in `s3://<warehouse>/benchmarks/<RunId>/`:

| File | Contents |
|---|---|
| `processing_delay.csv` | the headline metric, `commit_ts - last_updated_at` from gold, every row, identical on all five engines |
| `invariants.csv` | fold correctness, `sum(gold.trade_count)` against `count(silver.trades)` |
| `dedupe.csv` | silver dedupe correctness |
| `latency_percentiles.csv` | per-hop delay from the sampled Kafka emit chain |
| `correctness.csv` | rows stored per engine against the Kafka end offset |

Per-hop latency is sampled, on the same records at the same rate on every engine
(`trade_id % 997` on bronze and silver, `account_id % 97` on gold). The two families
still emit differently: Flink sends one message per sampled record, while Spark sends
the oldest and newest sampled event of each committed batch, because `observe()` is
aggregate-only and a per-row emit costs a second pass over the batch. Every message
carries `sample_kind` so the two are separable and are not pooled into one percentile
by accident.

### Sizing

| | 1k/s (measured) | 10k/s (estimate) | 30k/s (estimate) |
|---|---|---|---|
| Total vCPU demand | ~82 across 5 engines | ~90 to 110 | ~230 to 280 |
| NodePool `limits.cpu` | 96 | 96 | 256 |
| Kafka brokers | 3 | 3 | 5 |
| `app.public.trades` partitions | 3 | 12 | 24 to 36 |
| Spark executors, cores, memory | 1 x 1 x 2g, silver 3g; Hudi 2 x 2 x 4g | 2 x 2 x 4g | 4 x 2 x 8g |
| Flink TM cpu x mem | 2 x 4096m | 4 x 8192m | 8 x 16384m |
| Checkpoint interval | 10s | 10s | 30s |

The silver jobs carry 3g rather than 2g as headroom after the executor OOMKills, and
Hudi is wider throughout because its upsert path is the heaviest in the set. Neither
number is a measured high-water mark; see `tests/executor_memory_soak.py` for what the
soak did and did not reproduce.

Kafka partitions are the parallelism ceiling: a source cannot use more parallelism
than there are partitions, so raising executor counts without raising partitions buys
nothing. At high rates, checkpoint commit overhead grows with volume, and sub-30s
intervals start to dominate p99.

Above 10k/s a single Debezium replication slot becomes the limit, which is per slot
and combined across the publication's tables. Going further means sharding
publications or having the generator write to Kafka directly.

## Debugging a live run

The run workflow tears the cluster down when it finishes, so anything you want to
understand has to be looked at while it is up, or read out of the diagnostics bundle
afterwards.

### Getting a shell on the cluster

`kubectl` needs two things that are easy to miss. First, the cluster only admits the
GitHub deploy role that created it, so your own SSO identity has no access until you
add an entry for it:

```bash
export AWS_PROFILE=streaming-comparisons AWS_REGION=eu-west-1
CLUSTER=sc-iter                    # the run_id: sc-<run_id>
ROLE=$(aws iam list-roles \
  --query "Roles[?contains(RoleName,'AWSReservedSSO_AdministratorAccess')].Arn" \
  --output text | head -1)

aws eks create-access-entry --cluster-name "$CLUSTER" --principal-arn "$ROLE" --type STANDARD
aws eks associate-access-policy --cluster-name "$CLUSTER" --principal-arn "$ROLE" \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=cluster

aws eks update-kubeconfig --name "$CLUSTER"
```

An access entry is additive auth config and cannot disturb running pods, so it is safe
mid-run. It disappears with the cluster at teardown.

Second, every AWS command needs the profile. There is no `default` profile in this
setup, so a bare `aws ...` fails with `NoCredentials` even straight after a successful
`aws sso login`.

### What is running, and what is not

```bash
kubectl -n spark get sparkapplications \
  -o custom-columns='NAME:.metadata.name,STATE:.status.applicationState.state,ATTEMPTS:.status.executionAttempts'
kubectl -n flink get flinkdeployment \
  -o custom-columns='NAME:.metadata.name,LIFECYCLE:.status.lifecycleState'
```

`ATTEMPTS` is the number to watch. A job in `RUNNING` on attempt 4 is crash-looping and
recovering, which the state alone does not tell you.

`FlinkDeployment` reports the deployment, not the SQL jobs. For those, ask the
JobManager directly:

```bash
JM=$(kubectl -n flink get pods --no-headers | awk '$1 ~ /^flink-paimon-[0-9a-f]{8,}/{print $1}')
kubectl -n flink exec "$JM" -- curl -s localhost:8081/jobs/overview
```

### Why something died

```bash
# anything not healthy, and anything that has restarted
kubectl get pods -A --no-headers | awk '$4!="Running" && $4!="Completed"'
kubectl get pods -A --no-headers | awk '$5+0>0'

# the termination reason, which is where OOMKilled and exit 137 live
kubectl -n spark get pod <pod> -o jsonpath='{.status.containerStatuses[0].state.terminated}'

# the error message the operator kept
kubectl -n spark get sparkapplication <app> -o jsonpath='{.status.applicationState.errorMessage}'

kubectl get events -A --field-selector type=Warning --sort-by=.lastTimestamp | tail -20
```

A Spark stack trace is hundreds of frames deep, so grep for the message rather than
paging the log, and read the FIRST error rather than the last: a late failure is often
a rollback or cleanup failing after the real one.

```bash
kubectl -n spark logs <app>-driver --tail=-1 > /tmp/driver.log
grep -nE "ERROR|Exception:|Caused by" /tmp/driver.log | grep -vE "^\s+at " | head
```

Executor pods carry the evidence for an executor death, and Spark deletes them by
default. The manifests set `spark.kubernetes.executor.deleteOnTermination=false` so they
survive long enough to inspect. Note the host Spark reports for a lost executor is the
**pod** IP, not the node IP, since the VPC CNI allocates pod addresses from the node
subnet. Looking for it in `get nodes` will find nothing and suggest a node was lost when
none was.

### A Hudi table that will not commit

If a Hudi job crash-loops and its driver reports a rollback failing, the table has an
instant that never completed. Count only instants with no completed file: Hudi keeps
`.requested` and `.inflight` beside completed ones too, so a raw grep overstates it
badly (18 markers on a table that had 2 real problems).

```bash
aws s3 ls "s3://$BUCKET/hudi/gold/open_positions/.hoodie/" | awk '{print $4}' \
  | grep -E "^[0-9]+\." | python3 -c '
import sys, re, collections
by = collections.defaultdict(set)
for n in (l.strip() for l in sys.stdin):
    m = re.match(r"^(\d+)\.([a-z]+)(?:\.(requested|inflight))?$", n)
    if m: by[(m.group(1), m.group(2))].add(m.group(3) or "COMPLETE")
for k, v in by.items():
    if "COMPLETE" not in v: print(f"incomplete: {k[0]}.{k[1]}")'
```

Delete the `.requested` and `.inflight` markers of those instants and the write markers
under `.hoodie/.temp/<instant>/`, then restart the job. On a live run that took gold from
crash-looping to committing on the first attempt.

The rollback itself fails with `Wrong FS: s3a://..., expected: file:///` from Hudi's V1
rollback helper, and no Spark-side configuration avoids it. See
`tests/hudi_rollback_repro.py` for what was tried. A table only reaches this state when a
commit dies mid-flight, so the real prevention is upstream.

### Was the node taken away?

```bash
kubectl get nodes -L karpenter.sh/capacity-type
kubectl get nodeclaims -o custom-columns=\
'NAME:.metadata.name,TYPE:.metadata.labels.karpenter\.sh/capacity-type,NODE:.status.nodeName'
kubectl -n kube-system logs -l app.kubernetes.io/name=karpenter --tail=4000 \
  | grep -E "disrupting|tainted|deleted node|interrupt"
```

`disrupting node(s)` followed by `tainted node` and `deleted node` is Karpenter removing
a node. The NodePool is set to `consolidationPolicy: WhenEmpty` so it should only ever
remove empty ones; anything else there is worth investigating. Spot reclaim is a
separate path that this policy cannot prevent.

### Is data actually flowing?

```bash
# source rate, run it twice and difference the offsets
kubectl -n kafka exec trades-dual-role-0 -- \
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic app.public.trades \
  | awk -F: '{s+=$3} END {print s}'

# per-engine write recency, no cluster access needed
for p in delta iceberg hudi; do
  aws s3api list-objects-v2 --bucket streaming-comparison-amc-warehouse --prefix "$p/" \
    --query 'sort_by(Contents,&LastModified)[-1].[LastModified,Key]' --output text
done
```

The Flink engines write to the `-paimon` bucket, not the warehouse bucket, so an empty
`paimon/` or `fluss/` prefix in the warehouse bucket is expected rather than a fault.

### Reading the run from outside

Workflow logs for a job still in progress are only served through the API, and only up
to what has been flushed:

```bash
gh run list --limit 3
JID=$(gh run view <run-id> --json jobs --jq '.jobs[] | select(.name=="run") | .databaseId')
gh api "repos/<owner>/<repo>/actions/jobs/$JID/logs" > /tmp/job.log
```

`gh run view --log` returns nothing until the job finishes.

## Testing

```bash
make test        # offline: manifests, schemas, write paths, workflow shell, meta-tests
make test-all    # adds Docker: Flink and Fluss SQL compile, SCD2 and gold folds
                 # against real Delta, Iceberg, Hudi tables, dedupe across restarts,
                 # Glue registration against a local AWS emulator, executor memory
```

Every checker has a meta-test that feeds it a known-bad input and requires it to
fail, so a checker that stops checking is caught rather than passing quietly. Fixtures
are addressed by content, never by line number, for the same reason.

## Layout

```
jobs/           pipeline code, one directory per engine, plus jobs/_shared
generators/     synthetic trade and account CDC load
infra/aws/      Terraform, k8s manifests, run scripts
infra/          local Postgres, Strimzi, MinIO, Kafka Connect
docker/         images, including the latency exporter
tests/          checkers, integration tests, meta-tests
.github/        the run and teardown workflows
```
