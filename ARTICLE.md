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

## Laying the tables out, and why Hudi gets a different answer

Every format in this comparison wants the same thing from you, which is a way to avoid
reading the whole table to find a few rows. They just spell it differently, and the
spelling matters more than it looks.

The two trades tables were easy. Trades are immutable events with a timestamp, so
Iceberg partitions by `days(executed_at)`, Delta clusters on the same column, Hudi uses a
date partition path, and the Flink engines bucket by primary key. Gold is easy for the
same reason: positions are read per symbol, symbol has maybe a hundred values, and a
symbol never changes, so it partitions cleanly.

`silver.accounts` is the awkward one, and it went unlaid-out on two engines for months
without anyone noticing.

It is an SCD2 dimension: every version of every account, with validity ranges, one row
per account marked current. There is no date to split on and no low-cardinality attribute
that means anything. The obvious candidate is `account_id`, but with a thousand accounts
that is a thousand directories holding a few rows each, which trades one bad read pattern
for another. The other obvious candidate is `is_current`, which would put the ~1000 live
rows in one place and all the history in the other.

`is_current` is a trap, and it is worth being explicit about why. It is a **mutable**
field. Superseding an account flips its old row from true to false, and under a
directory-based layout that is not an update, it is a physical move: delete from one
partition, insert into the other. Every single update churns files. The rule that falls
out of this, and it generalises past this project, is that you partition on what a row
**is**, never on what a row currently **means**.

So there is no good partition column, and the layout has to come from somewhere else.

### Bloom versus bucket, which is where it gets interesting

Hudi has a knob the others do not, because Hudi keeps an explicit index mapping record
keys to the file group holding them. That index has implementations, and the choice
between two of them is a genuine engineering decision rather than a default worth
accepting.

**Bloom** is the default. Each base file carries a bloom filter of its keys in the
footer, plus the min and max key it holds. To locate an incoming key, Hudi narrows to
files whose key range could contain it, probes their bloom filters, and then, because a
bloom filter gives false positives and never false negatives, actually opens the
candidate files to confirm. It is a probabilistic funnel that ends in real reads.

The appeal is that it assumes nothing. Any key distribution, any file layout, no decision
required up front. The cost is that the work scales with how many files it has to
consider, and an SCD2 table only grows: every version of every account is another row,
in another file, whose bloom filter is another probe. The lookup gets slower for no
reason other than the passage of time.

**Bucket** replaces the search with arithmetic. Fix a bucket count at table creation,
hash the key field, and each bucket maps to exactly one file group. Locating a key is
computing a hash. Nothing is probed, no filters are read, no candidate files are opened
to rule them out, and it costs the same on day one as it does after a million versions.

The trade is real and worth stating rather than glossing:

- **The bucket count is close to permanent.** It is chosen before the data exists and
  changing it later means rewriting the table. Bloom needs no such foresight.
- **Skew goes straight through.** Hashing spreads keys evenly only if the keys are
  evenly used. One account taking most of the traffic puts most of the writes on one
  file group, and bucketing cannot help, whereas bloom would spread naturally across
  files.
- **It is a bet that the key distribution is known and stable.** For an arbitrary lake
  table that is presumptuous. For a dimension of a thousand accounts it is simply true.

That last point is what settles it here. This is a bounded, well-understood key space,
read by exact key on every micro-batch, in a table that grows forever while the number of
distinct keys does not. That is close to the ideal case for bucketing and close to the
worst case for bloom, where the probe cost grows with history that the lookup never
wants. Sixteen buckets over a thousand accounts is about sixty accounts each, which keeps
the file groups a sensible size without inventing parallelism that a small executor cannot
use anyway.

Delta was already right by accident, clustering on `account_id`, since liquid clustering
is adaptive and needs no partition column at all. The Flink engines get it free, because
a primary key table is bucketed on the primary key by construction.

Which leaves Iceberg, and Iceberg is where the argument fell over.

### The same idea, measured, losing by a factor of eleven

Iceberg has no record index, so the apparent equivalent is `PARTITIONED BY (bucket(16,
account_id))`, a hidden partition transform: the same hashing idea expressed as physical
layout rather than as an index. I shipped it on the strength of the reasoning above, which
is the mistake, and the reasoning survived contact with a benchmark for about twenty
minutes. Building the same table both ways on real S3, 1000 accounts and 36,000 versions:

| layout | data files | current-row lookup | MERGE |
| --- | --- | --- | --- |
| none | 120 | 1384ms | 5692ms |
| `bucket(16, account_id)` | 1920 | 15159ms | 28825ms |

Eleven times worse on the lookup, five times worse on the MERGE.

The mechanism is the file count, and it is entirely a consequence of the table being
**streamed** rather than loaded. A bucketed append has to write one file per bucket it
touches. The job commits a micro-batch every fifteen seconds, so 120 micro-batches leave
1920 files where an unbucketed table leaves 120. Then the pruning that is supposed to pay
for those files never arrives, because a batch of 75 randomly distributed account ids
hashes into all sixteen buckets. Every bucket is read on every batch. All of the cost,
none of the benefit.

The lesson is not "bucketing is bad". Bucketing pays when a query touches few buckets, and
the argument two sections up is still correct on its own terms: a bounded key space read by
exact key really is bucketing's good case. What that argument left out is that per-batch
SCD2 maintenance touches *every* bucket by construction, and that the writer is a stream
committing every fifteen seconds rather than a batch job committing once. Hudi's bucket
index is genuinely a different mechanism, an index over stable file groups where an upsert
lands in the existing group's log file, so it does not multiply files this way and it
stays. The Iceberg version is reverted.

There is a smaller lesson underneath, which is that the first version of this benchmark
also said bucketing lost, for completely the wrong reason. It built each table in a single
append, so it was comparing one large file against sixteen, and it had quietly handed the
unbucketed table a compaction that production never performs. Two benchmarks, the same
verdict, and only one of them measuring the thing. Agreeing with a result is not the same
as checking it.

The real lever for `silver.accounts` is compaction, which attacks the file count directly
instead of trading it for pruning that this access pattern cannot use.

### What this did not fix

Worth saying plainly, because the tempting story is that the unorganised table explained
the slow pipeline and it does not. `iceberg-silver-accounts` was running 47 to 51 second
batches against a 15 second trigger, and it was indeed the only silver or gold table
anywhere with no layout at all, which is a satisfying coincidence and was never a proven
cause. It is now a disproven one: the unlaid-out table does the lookup in 1.4s and the
MERGE in 5.7s against real S3, nowhere near a 47 second batch. Whatever is costing that
time is somewhere else, and the layout change would have made it worse rather than
explaining it.

The same applies to a related change. Shuffle partitions had been sitting on Spark's
default of 200 while these executors run a single core, so every shuffle stage produced
200 serialised tasks over trivial data, and streaming queries disable adaptive execution
so nothing coalesced them. That is obviously wrong and now set to 8. It is also worth
about three percent, measured, which is a useful reminder that the obviously-wrong thing
and the expensive thing are frequently not the same thing.

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
