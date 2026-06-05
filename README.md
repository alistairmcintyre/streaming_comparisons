# streaming_comparisons

Test various streaming features across Spark and Flink.

## Objectives

1. Create Python function to generate item inventory changes in a Postgres database
2. Create Python function to generate item attribute detail changes in a Postgres database
3. Create Debezium CDC connectors to stream changes to Kafka topics
4. Create Spark streaming job to consume item inventory changes and write to Iceberg bronze table (append, partitioned on `days(event_ts)`)
5. Create Flink streaming job to consume item inventory changes and write to Iceberg bronze table (append, partitioned on `days(event_ts)`)
6. Create Spark streaming job to consume data from bronze Spark table and upsert into Iceberg silver table (partitioned on `event_date`)
7. Create Flink streaming job to consume data from bronze Flink table and upsert into Iceberg silver table (partitioned on `event_date`)
8. Reporting UI that updates every 30 seconds — shows item inventory counts for 5 selected items from the Spark silver table and Flink silver table side by side
9. Include processing delay per engine (end-to-end, commit lag, freshness)
10. Include Iceberg snapshot stats — file counts for Spark tables vs Flink tables
11. Reconciliation panel — Kafka latest-value cache vs Spark silver vs Flink silver
12. Runs fully locally via Docker

---

## Iceberg Write Modes in Spark Streaming

| Mode | How it works | When Spark writes it | Read cost | Write cost at 20s cadence |
|---|---|---|---|---|
| **Copy-on-Write (CoW)** | On every MERGE/UPDATE, rewrites entire Parquet files containing touched rows. Old files deleted, new files committed atomically. | Default in Iceberg v1; also v2 if `write.*.mode` is not set | Fast — no delete files to resolve | Catastrophic — full file rewrites every batch |
| **Merge-on-Read (MoR) with equality deletes** | MERGE writes small equality-delete files (`delete where item_id=42`) alongside new data files. Old data files stay untouched. | v2 tables with `write.delete.mode=merge-on-read` (used in this project) | Moderate — must apply equality deletes at scan time | Cheap per-write; delete files accumulate over time |
| **MoR after position delete compaction** | `rewrite_position_delete_files` converts equality deletes into position deletes (`delete row at file X, offset Y`), cheaper to resolve at read time | Produced by the compactor, not the streaming job | Fastest MoR variant | N/A — compaction is offline |
| **`foreachBatch` + `MERGE INTO`** | Not a table format — the Spark Structured Streaming delivery pattern. Each micro-batch triggers a `MERGE INTO`. The table format (CoW or MoR) controls what that MERGE physically writes. | Spark silver jobs in this project, trigger=20s | Depends on table mode | Depends on table mode |

---

## `foreachBatch` + `MERGE INTO` — Full Table Mode Breakdown

`foreachBatch` is just the delivery mechanism. The interesting question is what physically happens inside Iceberg when `MERGE INTO` executes. Controlled by three table properties (settable independently):

```
write.delete.mode  — controls DELETE and the delete side of MERGE WHEN MATCHED ... DELETE
write.update.mode  — controls UPDATE and the update side of MERGE WHEN MATCHED ... UPDATE
write.merge.mode   — controls the overall MERGE INTO (overrides the above two for merges)
```

In practice for a streaming MERGE, set all three to the same value.

### Option 1: Copy-on-Write (CoW)

```python
"write.delete.mode": "copy-on-write",
"write.update.mode": "copy-on-write",
"write.merge.mode":  "copy-on-write",
```

Spark finds every data file containing a touched row, reads it fully, rewrites it with the change applied, and atomically swaps the new file into the snapshot. Old files are orphaned (cleaned by `expire_snapshots`).

- **Read performance:** Excellent — always clean Parquet, no delete files to resolve
- **Write performance at streaming cadence:** Terrible — rewrites entire files even if 1 row changed; amplification is proportional to file size, not change volume
- **When to use:** Large infrequent batch merges (hourly/daily). Not streaming.

### Option 2: Merge-on-Read (MoR)

```python
"write.delete.mode": "merge-on-read",
"write.update.mode": "merge-on-read",
"write.merge.mode":  "merge-on-read",
```

Spark does not touch existing data files. Per batch it writes: (1) an equality delete file listing changed/deleted `item_id`s, and (2) new data files for updated/inserted rows. At read time, Iceberg applies the equality deletes as a filter on top of data files.

- **Read performance:** Degrades over time without compaction — scans must open and evaluate every accumulated equality delete file
- **Write performance at streaming cadence:** Cheap — cost is proportional to change volume, not table size
- **Used by the silver tables in this project**

Two sub-variants of MoR delete files:

| | Equality deletes | Position deletes |
|---|---|---|
| **Written by** | `MERGE INTO` on a MoR table | `rewrite_position_delete_files` compaction |
| **Content** | "delete rows matching predicate" e.g. `item_id = 42` | "delete row at byte offset Y in file X" |
| **Read resolution** | Must evaluate predicate against every row in every data file | Direct seek — much cheaper |
| **Iceberg version** | v2 only | v2 only |

The compactor converts equality deletes → position deletes, then eventually merges everything back into clean data files via `rewrite_data_files`.

### Option 3: Mixed modes

`write.merge.mode` overrides the other two for `MERGE INTO` statements, so the only useful split for a streaming MERGE job is:

```python
"write.delete.mode": "merge-on-read",
"write.update.mode": "copy-on-write",  # updates rewrite files — fewer delete files, better read perf
"write.merge.mode":  "merge-on-read",  # MERGE uses MoR overall
```

Worth experimenting with on update-heavy workloads to reduce equality delete accumulation, at the cost of slightly higher write amplification on the update side.

### Trigger interval × write mode interaction

| Trigger | Mode | Delete files after 1hr | Notes |
|---|---|---|---|
| 20s | CoW | 0 (clean rewrites) | ~180 full file rewrites per hour |
| 20s | MoR | ~180 equality delete files | Needs compaction every ~10 min |
| 60s | MoR | ~60 equality delete files | Compaction less urgent |
| 20s | MoR + compaction every 10min | Low steady state | What this project uses |

The 20s trigger and 10min compaction interval mean compaction runs every ~30 batches — enough delete files to make it worthwhile, not so many that reads degrade between runs.

To switch silver tables to CoW for comparison, edit `ddl/tables.py:SILVER_PROPERTIES` and run `make clean && make all`. The snapshot stats panel in the UI will show delete file count drop to zero with increased data file churn.

---

## Questions and Answers

### Q1: When does CoW write amplification justify switching to MoR?

The crossover is driven by **change density** — what fraction of rows in a file are touched per batch.

- **Low density** (e.g. 5 rows changed in a 128MB / 1M-row file): CoW rewrites 128MB to change 5 rows. MoR writes a ~200-byte delete file. MoR wins by orders of magnitude.
- **High density** (e.g. 800k of 1M rows changed): CoW rewrites the file once. MoR must evaluate 800k equality deletes against 1M rows at read time — effectively a full join. CoW wins.

**Practical crossover: ~5–10% change density per file per batch.** For a streaming upsert hitting a subset of hot items you're typically at <1% — MoR is the right choice.

---

### Q2: Checkpoint-to-commit latency — Spark micro-batch vs Flink Iceberg sink

**Spark `foreachBatch`:**
```
Kafka messages arrive
  → trigger fires (every 20s)
  → batch reads Iceberg bronze (snapshot scan)
  → foreachBatch executes MERGE INTO
  → Iceberg commit (REST catalog PUT)
  → checkpoint written to S3
  → next trigger starts
```
The Iceberg commit happens *inside* the batch, before the checkpoint. A crash after commit but before checkpoint causes the batch to replay — the MERGE is idempotent (guarded by `source_updated_at`) but produces a duplicate snapshot. **Effective commit latency: ~20–35s** (trigger interval + MERGE execution time).

**Flink Iceberg sink:**
```
Kafka messages arrive continuously
  → state updated in RocksDB per record (no batching)
  → checkpoint barrier flows through DAG (every 10s)
  → on checkpoint completion: sink flushes write buffer and commits to REST catalog
  → checkpoint marked complete in JobManager
```
The Iceberg commit and Flink checkpoint are atomic — commit only happens when checkpoint succeeds, checkpoint only completes when commit succeeds. Failure causes replay from the previous checkpoint with exactly-once semantics. **Effective commit latency: ~10–15s** (checkpoint interval).

In the UI you should consistently see Flink freshness ~10–15s vs Spark ~25–35s.

---

### Q3: Exactly-once guarantees when switching from append to MERGE

**Append (bronze tables):** If a batch replays on crash recovery, the same rows are appended again — bronze gets duplicates. Acceptable for a raw CDC log; dedupe downstream.

**MERGE (silver tables):**

- **Flink:** Exactly-once preserved. Iceberg commit is gated on checkpoint completing. Replay re-applies the same RocksDB state, producing the same upsert output.
- **Spark `foreachBatch`:** At-least-once in practice. A crash after the Iceberg commit but before the checkpoint write causes the batch to re-apply. The MERGE is idempotent (the `source_updated_at >= t.source_updated_at` guard means stale replays hit `WHEN MATCHED` but do no harm), so results are correct — but you get an extra no-op snapshot.

The subtle case: if a MERGE commits but the Spark checkpoint doesn't write (S3 flakiness), re-inserted rows that already exist will correctly route to `WHEN MATCHED` on replay. No data corruption, just an extra snapshot.

---

### Q4: What Spark/Flink commit to Iceberg, when, and how snapshot atomicity interacts with engine checkpoints

**What a commit contains:**

Every Iceberg commit is a new **snapshot** — an atomic pointer swap in the catalog. A snapshot references a manifest list → manifest files → data files + delete files added/removed in that commit. The REST catalog uses optimistic locking + a JDBC transaction to swap the pointer atomically. Nothing is modified in place; old files stay readable until `expire_snapshots`.

**Spark commit sequence:**
1. `foreachBatch` executes
2. `MERGE INTO` runs → Iceberg writes new data files + equality delete files to S3
3. Iceberg builds new manifest + manifest list
4. REST catalog atomically swaps metadata pointer to new snapshot
5. Batch returns → Spark writes checkpoint offset to `s3a://warehouse/_chk/`
6. Next trigger starts

Steps 4 and 5 are not atomic with each other — this is the at-least-once gap.

**Flink commit sequence:**
1. Checkpoint barrier injected at Kafka source
2. Barrier flows downstream through all operators
3. Barrier reaches Iceberg sink → sink flushes in-flight write buffer; data + delete files land on S3
4. Each subtask reports state (including pending Iceberg files) to checkpoint coordinator
5. All subtasks complete → coordinator marks checkpoint complete
6. Iceberg sink's `notifyCheckpointComplete` fires → commits to REST catalog
7. Kafka offsets committed only after step 6

Steps 6–7 are post-checkpoint. Failure between them causes replay from the last checkpoint, but the pending files list is in the checkpoint state so the commit is retried exactly once. This is the **two-phase commit (2PC)** pattern.

**Commit contention:** Iceberg uses optimistic concurrency — two jobs committing to the same table simultaneously have one succeed and one retry with `CommitFailedException`. The compactor and streaming job collide regularly on silver tables; `commit.retry.num-retries=10` resolves this cleanly.

---

### Q5–Q8: Throughput scaling

| Rate | Debezium | Kafka | Spark | Flink | Iceberg |
|---|---|---|---|---|---|
| **200/s** | Single connector, 1 partition | Fine | `local[2]`, 20s trigger | Parallelism 1 | No changes needed |
| **1,000/s** | Single connector | 4–8 partitions | `local[4]`, larger batches | Parallelism 2–4 | No changes needed |
| **10,000/s** | Near WAL limit; needs partition routing via SMT | 10–20 partitions | Needs cluster + partition-pruned MERGE | Parallelism 4–8, preferred engine | Monitor commit contention |
| **100,000/s** | Not viable from single Postgres WAL | 20+ partitions, producer batching | Not viable for MERGE | Flink CDC direct (bypasses Debezium), parallelism 16–32 | Compacted-topic snapshot approach |

**Detail by scale:**

**200/s — current architecture works as-is.** 200 msg/s × 20s = 4,000 rows per Spark batch. MERGE executes in 2–5s. All jobs fit in `local[2]` with parallelism 1.

**1,000/s — tuning only, same architecture.** Increase topic partitions to 4–8 for downstream parallelism. Spark: `local[4]`, 10s trigger gives ~10,000 rows/batch. Flink: parallelism 2–4. Iceberg unchanged — commit rate is driven by trigger/checkpoint interval, not message rate.

**10,000/s — architecture changes required:**
- Debezium: single connector near Postgres WAL limit (~50–100MB/s). Need partition routing by `item_id` hash (Debezium SMT `PartitioningRoutingStrategy`) to allow parallel consumers
- Postgres: 10k writes/s approaches the practical limit for a single instance with logical replication. PgBouncer + table partitioning needed
- Spark: 10k msg/s × 20s = 200k rows/batch. MERGE on a single node takes 30–60s, exceeding the trigger. Needs a proper cluster (driver + 2–4 executors) and partition-pruned MERGE hitting the `event_date` column
- Flink: still handles this well at parallelism 4–8

**100,000/s — fundamentally different approach required:**
- **Postgres WAL** at 100k writes/s generates ~500MB/s–1GB/s. Debezium cannot consume a single slot fast enough; replication lag grows unboundedly. Options: shard across multiple Postgres instances each with their own Debezium connector, or replace Postgres with a write-optimised source (ScyllaDB, Kafka-native producer) that bypasses WAL entirely
- **Spark MERGE** is not viable: 100k × 20s = 2M rows/batch. MERGE planning alone exceeds 20s on any single node
- **Flink with Flink CDC 3.x**: eliminates the Debezium→Kafka hop entirely, reads directly from Postgres binlog (or ScyllaDB CDC). Parallelism 16–32 across multiple TaskManagers. Iceberg sink commit rate stays constant (checkpoint-driven) regardless of message rate — this is the key architectural advantage
- **Alternative — compacted topics + periodic snapshot**: use Kafka log compaction to maintain latest-value-per-key natively, then bulk-load the compacted topic into Iceberg every few minutes. Trades real-time freshness for dramatically simpler infrastructure and higher throughput. Silver becomes a periodic materialisation rather than a continuous upsert target.

---

## Make Commands

### Infrastructure

| Command | What it does |
|---|---|
| `make build` | Builds all Docker images (Spark, Flink, Streamlit) without starting anything. Run this first to pre-pull and compile images, or after editing a Dockerfile. |
| `make up` | Starts the core infrastructure services in detached mode: Postgres (app + catalog), Kafka, MinIO, MinIO bucket init, Iceberg REST catalog, Kafka Connect (Debezium). Does not start any streaming jobs or the UI. |
| `make wait` | Polls all service health endpoints (`/v1/config`, `/connectors`, MinIO health, Flink overview) and blocks until each responds. Run after `make up` before any setup steps. |

### One-time Setup

| Command | What it does |
|---|---|
| `make create-tables` | Runs the `ddl-init` container which executes `ddl/create_namespaces.py` via PyIceberg. Creates the `bronze` and `silver` namespaces and all 8 Iceberg tables with correct schemas, partition specs, and table properties (format-version=2, MoR settings, upsert identifier fields). Safe to re-run — skips tables that already exist. |
| `make register-connectors` | POSTs the two Debezium connector configs (`pg-item-inventory.json`, `pg-item-attributes.json`) to the Kafka Connect REST API. Debezium immediately begins an initial snapshot of all 1,000 seed rows into Kafka, then switches to streaming WAL changes. Uses HTTP PUT (upsert), so safe to re-run. |

### Start Pipelines

| Command | What it does |
|---|---|
| `make start-generators` | Starts the two Python generator containers (`generator-inventory`, `generator-attributes`) which write continuous changes to Postgres at configurable rates (default: 5 evt/s inventory, 1 evt/s attributes). These trigger Debezium CDC events into Kafka. |
| `make start-spark` | Starts all four Spark streaming job containers: `spark-bronze-inv`, `spark-bronze-attr` (Kafka → Iceberg bronze, append, 15s trigger), `spark-silver-inv`, `spark-silver-attr` (bronze → Iceberg silver, `foreachBatch` + `MERGE INTO`, 20s trigger, MoR). Each job checkpoints to MinIO. |
| `make start-flink` | Starts the Flink JobManager and TaskManager, waits 10s, then starts `flink-submitter` which runs `jobs/flink/submit.sh` to submit all four Flink SQL jobs (bronze append + silver upsert via `debezium-json` format, checkpoint every 10s). Jobs are visible in the Flink UI at `http://localhost:8081`. |
| `make start-compactor` | Starts the `compactor` container which runs `jobs/spark/maintenance_compaction.py` — a loop every 10 minutes calling `rewrite_data_files`, `rewrite_position_delete_files`, and `expire_snapshots` on all 8 tables. Prevents delete file accumulation from degrading silver table read performance. |

### UI

| Command | What it does |
|---|---|
| `make ui` | Starts the Streamlit UI container and prints service URLs. Opens `http://localhost:8501` (Streamlit), `http://localhost:9001` (MinIO console), `http://localhost:8081` (Flink UI), `http://localhost:8083` (Kafka Connect REST). |

### Full Bring-up

| Command | What it does |
|---|---|
| `make all` | Runs the full startup sequence in order: `up` → `wait` → `create-tables` → `register-connectors` → 15s sleep (Debezium snapshot propagation) → `start-generators` → `start-spark` → `start-flink` → `start-compactor` → `ui`. Single command to go from zero to a running stack. |

### Monitoring

| Command | What it does |
|---|---|
| `make status` | Prints Debezium connector states (RUNNING / FAILED / PAUSED) by querying `http://localhost:8083/connectors?expand=status`, followed by `docker compose ps` showing all container states. |
| `make logs` | Tails the last 50 lines of logs from all containers and follows. Ctrl+C to stop. |
| `make logs-spark` | Tails logs from the four Spark job containers only. Useful for debugging MERGE failures or checkpoint errors. |
| `make logs-flink` | Tails logs from `flink-jobmanager`, `flink-taskmanager`, and `flink-submitter`. Use alongside the Flink UI at `http://localhost:8081` for job-level detail. |
| `make logs-connect` | Tails Kafka Connect / Debezium logs. First place to look if CDC events stop appearing in Kafka. |

### Teardown

| Command | What it does |
|---|---|
| `make down` | Stops and removes all containers. Preserves Docker volumes (Postgres data, MinIO data, checkpoints) so the stack can be restarted without re-running setup. |
| `make clean` | Stops and removes all containers **and all volumes** (`docker compose down -v`). Destructive — wipes all Iceberg data, Postgres data, Kafka offsets, and checkpoints. Use when you want a completely fresh start, e.g. after changing table schemas in `ddl/tables.py`. |

---

## Manual CDC Test — Item Attributes (Spark only)

Tests the full append → update → delete lifecycle for a single row through bronze and silver. Uses `item_id = 9001` (outside the 1–1000 seed range so generators won't interfere).

Prerequisites: `make up && make wait && make create-tables && make register-connectors && make start-spark`. Wait ~30s after starting Spark for the Debezium snapshot to propagate through bronze.

### Terminal 1 — Postgres writes

```bash
docker compose exec postgres-app psql -U app -d appdb
```

Run each statement, then switch to Terminal 2 to observe. Wait ~25s between changes to allow a full Spark trigger cycle (bronze 15s + silver 20s).

```sql
-- Step 1: INSERT
INSERT INTO item_attributes (item_id, name, price, category)
VALUES (1, 'Test Widget', 9.99, 'tools');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (2, 'tv', 199.99, 'IT equipment');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (3, 'speakers', 55.00, 'IT equipment');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (4, 'mouse', 35.00, 'IT equipment');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (5, 'mouse pad', 5.00, 'IT equipment');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (6, 'phone', 465.00, 'IT equipment');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (7, 'Headphones', 45.00, 'electronics');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (9, 'watch', 455.00, 'electronics');

INSERT INTO item_attributes (item_id, name, price, category)
VALUES (10, 'nike air max', 99.00, 'trainers');

-- Step 2: UPDATE price
UPDATE item_attributes SET price = 49.99, updated_at = now()
WHERE item_id = 1;

-- Step 3: UPDATE category
UPDATE item_attributes SET category = 'electronics', updated_at = now()
WHERE item_id = 2;

-- Step 4: DELETE
DELETE FROM item_attributes WHERE item_id = 2;



```

### Terminal 2 — Iceberg SQL queries

```bash
make sql
```

Run after each step above.

```sql
-- Current state in silver (SCD Type 1 — always 0 or 1 row per item_id)
SELECT item_id, name, price, category, source_updated_at, commit_ts
FROM rest.silver.item_attributes_spark
WHERE item_id = 9001;

-- Full history in bronze (append-only — grows by 1 row per CDC event)
SELECT op, item_id, name, price, category, source_updated_at, ingest_ts
FROM rest.bronze.item_attributes_spark
WHERE item_id = 9001
ORDER BY source_updated_at;

-- File counts by type (watch equality deletes accumulate on each UPDATE/DELETE)
-- content: 0 = DATA  1 = POSITION_DELETES  2 = EQUALITY_DELETES
SELECT content, count(*) AS file_count
FROM rest.silver.item_attributes_spark.files
GROUP BY content;

-- Latest snapshots (operation type + files added/removed per commit)
SELECT committed_at, operation, summary
FROM rest.silver.item_attributes_spark.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

### Expected results at each step

| Step | Silver row | Bronze rows | Equality delete files |
|---|---|---|---|
| After INSERT | 1 row — name=Test Widget, price=9.99, category=tools | 1 (`op='c'`) | 0 new |
| After UPDATE price | 1 row — price=49.99 | 2 (`op='c'`, `op='u'`) | +1 |
| After UPDATE category | 1 row — category=electronics | 3 (`op='u'`) | +1 |
| After DELETE | 0 rows | 4 (`op='d'`) | +1 (no new data file) |

Silver is SCD Type 1 — one current row, overwritten on update, removed on delete. Bronze is the full audit log and never loses rows.

### On-demand compaction

To see compaction collapse the accumulated equality delete files into clean data files without changing any row values:

```sql
CALL rest.system.rewrite_position_delete_files(table => 'rest.silver.item_attributes_spark');
CALL rest.system.rewrite_data_files(
  table => 'rest.silver.item_attributes_spark',
  options => map('delete-file-threshold', '1', 'min-input-files', '2')
);
```

After this runs, re-query `.files` — equality delete count drops to 0, a new snapshot appears with `operation = 'replace'`, and the silver row values are unchanged.

debugging:

flink
check job statuses
curl -s http://localhost:8081/jobs | python3 -m json.tool
curl -s http://localhost:8081/overview | python3 -m json.tool && docker compose ps flink-jobmanager flink-taskmanager flink-submitter
