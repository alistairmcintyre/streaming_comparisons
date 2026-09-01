# From one Iceberg limitation to a lakehouse benchmark

Notes for a write-up. The project started as a narrow question about Iceberg and
streaming reads. Answering it properly turned it into a comparison of five streaming
stacks on real infrastructure, and most of what I learned came from the second part.

## Where it started

The original question was small. I wanted a silver table holding one current row per
key, continuously updated from CDC, that a downstream streaming job could read as a
changelog to keep a gold aggregate current. Standard medallion shape, standard
requirement.

On Iceberg, it does not work.

A Flink upsert sink needs `upsert-enabled=true` to maintain one row per key. In that
mode it writes an equality delete alongside a data record for every row, inserts
included, because a streaming write cannot know whether the key already exists.
Any Iceberg commit that contains delete files is a `RowDelta`, and a `RowDelta`
commits with `operation = overwrite`, never `append`. Flink's Iceberg streaming source
will not emit a changelog from overwrite snapshots, so the downstream job reads
nothing at all.

The evidence was unambiguous once I looked at the snapshots directly. Gold stayed
empty for 300 seconds while silver was fully populated and perfectly readable in
batch. The only `append` snapshots on the silver table were empty checkpoint
heartbeats with zero added records. Every actual row had arrived inside an
`overwrite` commit.

Spark has the same problem by a different route: `foreachBatch` plus `MERGE INTO`
also produces overwrite snapshots.

So the answer is that you choose. Either silver is an append-only changelog and the
current view gets derived on read, or silver is a current view and gold has to be
batch. You cannot have one Iceberg table that is both.

I had assumed the limitation was about deletes. It is not. It applies to inserts too,
which is what makes it structural rather than an edge case.

## Why it became a benchmark

A negative result about one format is a blog post. The more useful question is what
the alternatives actually cost, and that cannot be answered by reading documentation,
because every format's documentation says it handles streaming upserts well.

The streaming-native formats exist precisely to close this gap. Paimon has
`changelog-producer`, Delta has Change Data Feed, Hudi has incremental queries. Each
gives a current-view table that is also readable as a changelog. On paper they all
solve it. The interesting part is what happens when you run them side by side, at a
realistic rate, on the same data, and try to operate them.

So the project grew into five pipelines: Spark with Delta, Spark with Iceberg, Spark
with Hudi, Flink with Paimon, and Flink with Fluss tiering into Paimon and Iceberg.
Same Postgres source, same Debezium CDC, same medallion shape, same gold metric.

Holding the shape constant is what makes it a comparison rather than five demos. The
SCD2 staging logic is shared code across the three Spark engines, so they cannot
drift on what SCD2 means; only the write mechanism differs. Delta and Iceberg use a
single MERGE, Hudi rides on a composite record key and precombine. That difference is
the thing being measured.

One asymmetry is real and stays in: Fluss has no bronze layer, because its primary-key
table is already the cleaned landing table. Inventing a bronze to make the table
counts match would misrepresent the engine, so the results record the hop count
instead.

## Taking it to AWS

Local Docker Compose was enough to find logic bugs and nothing else. The failures
that mattered only appeared with a real catalog, real object storage and a real
scheduler.

Runs are ephemeral and cost-capped. A GitHub Actions workflow creates an EKS cluster,
builds and pushes images, deploys, runs, snapshots results to S3 and destroys
everything. A Terraform-created EventBridge schedule fires a teardown Lambda at 2.5
hours whether or not the workflow is still alive. Every run is throwaway.

That constraint shaped the engineering more than any technical decision. When a run
costs money and 20 minutes of cluster build, you stop debugging interactively and
start making the run produce evidence. Diagnostics are collected before teardown, not
after a failure. Everything cheap that can fail early runs offline first: manifest
linting, schema parity, workflow shell parsing. Three runs failed in one day on bugs
that an offline check would have caught for nothing, and that is what the offline
suite exists for.

## What the failures actually were

This is the part I did not expect. The pipeline logic was rarely wrong. What broke
runs was version skew, image contents, catalog registration and Kubernetes
autoscaling.

**A stack overflow that only happens at scale.** The Hudi silver job pruned its
current-row lookup with `account_id IN (...)`, one literal per key in the batch.
Hudi's column-stats data skipping expands that into a per-literal chain of nested
predicates, and Catalyst resolves expressions recursively, so the driver stack
overflows. The recursion depth equals the number of distinct keys in the batch. With
four keys in a unit test it is invisible. In production, where a restart backlog
fills the batch with the entire key space, it is certain. The job failed, retried
into a larger backlog, and burned all ten restarts. The fix is a bounded predicate
plus a semi-join, so the pushed-down filter is two literals regardless of batch size.

**Athena reads columns from Glue, not from the metadata.** Paimon and Fluss tables
registered with an empty column list answered `SELECT count(*)` correctly, because
that only needs the snapshot, and failed `SELECT *` with `COLUMN_NOT_FOUND`. The
metadata file was fine the whole time. There is a tempting red herring here: Paimon
numbers Iceberg field ids from 0 where a native writer starts at 1. Re-registering
with 1-based ids changed nothing.

**Delta's deletion vectors and Athena DDL.** Enabling deletion vectors raises the
table protocol past what Athena's DDL engine accepts, so `CREATE EXTERNAL TABLE`
fails. Athena's query engine has read deletion vectors since 2024. Only DDL refuses,
so the tables register through the Glue API instead.

**Negative latency.** One pipeline reported latencies down to minus 998 seconds.
Flink's `UNIX_TIMESTAMP(string, format)` and `TO_TIMESTAMP(string, format)` disagree
on the `SSSSSS` pattern: it is a millisecond field padded to six digits, not a
microsecond fraction, so `.999999` was read as 999999 milliseconds and pushed events
into the future.

**Autoscaling as a correctness problem.** Two engines failed with
`ExecutorDeadException` and `MetadataFetchFailedException`, which both mean the
executor is gone and neither says why. Memory was the obvious suspect and it was
wrong: running the same workloads in a container capped at the real pod limit showed
8.4 million keys of RocksDB state peaking at half the limit. The actual cause was
Karpenter consolidating underutilized nodes every two minutes, and "underutilized"
describes a benchmark node for most of its life. With every Spark app running a
single executor, that is not a degraded query but a dead one. Beyond the failures, it
is a measurement problem: repacking nodes mid-run forces recomputation inside the
window being timed and can move a pod to a different instance type partway through
the comparison.

## Observations

**The format is not the whole story; the engine's changelog support is.** Flink does
retraction-aware aggregation natively, keeping the running aggregate in checkpointed
state and emitting retractions automatically. Spark has no equivalent, so every Spark
gold job hand-rolls a signed-delta MERGE and needs an idempotency stamp to survive
`foreachBatch` replays. The same table format is meaningfully easier to use under one
engine than the other.

**Most aggregations do not need any of this.** The hard machinery only applies when
the metric tracks mutable state. Trade counts and notional volume are immutable-event
metrics: aggregate the append stream with a watermark and skip current-view
maintenance entirely. Recognising which case you are in is the highest-leverage
decision in the design, and it comes before choosing a format.

**Measurement design is harder than measurement.** Making five engines produce
comparable numbers took more thought than making them run. Spark's `observe()` is
aggregate-only and emits once per committed batch; Flink SQL has no post-commit hook
and emits per record. Neither can be made to imitate the other cheaply, since a
per-row emit from Spark costs a second pass over the batch, which roughly doubles the
work of the job being measured. The resolution was to make the headline metric
something both families compute identically, `commit_ts - last_updated_at` read
straight out of the gold table, every row, no sampling, and to treat the per-hop
Kafka emit as a secondary signal that carries a label saying how it was sampled.

**Tests that cannot fail are worse than no tests.** A meta-test in this repo injected
a bug at a hard-coded line number. Adding lines above it made it inject nothing, so it
passed while checking nothing. Every checker here now has a meta-test that feeds it a
known-bad input and requires a failure, and fixtures address content rather than line
numbers.

**The cheap reproduction is usually available.** The Hudi stack overflow, which
burned a cluster run, reproduces locally in about 90 seconds with a rate source and no
Kafka. The executor memory question, which looked like it needed AWS, is really a
question about a cgroup limit, and `docker run --memory` answers it exactly. The
instinct to reach for the full environment was wrong more often than it was right.

## Still open

- Serialization as a measured dimension. Every engine currently consumes the same
  Debezium JSON, which is fair but inflates all absolute figures and compresses the
  gaps between engines. At higher rates a growing share of what is measured is Kafka
  and JSON parsing rather than the lake engine. Avro is the comparison to run.
- Schema evolution under load, and where a registry belongs in this shape.
- Fluss tiering lag against its live table, which currently makes one drift figure
  unusable as stated.
