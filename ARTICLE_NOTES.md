# Streaming Iceberg: Why Flink Can Do What Spark Can't

Notes for a Medium article on streaming reads and writes across Iceberg layers using Flink and Spark.

---

## The Core Insight

This project demonstrates something that is easy to miss when working with Iceberg and streaming engines: **Flink can stream-read from a silver Iceberg table that is being continuously written to by another Flink job. Spark cannot do this.**

The reason comes down to how each engine writes snapshots.

---

## Spark vs Flink: How They Write Iceberg Snapshots

### Spark — `overwrite` snapshots

When Spark Structured Streaming runs a `foreachBatch` + `MERGE INTO`, each micro-batch produces an Iceberg snapshot with `operation = overwrite`. An overwrite snapshot replaces existing data — it rewrites files and commits a new snapshot that supersedes the previous one.

From the perspective of an Iceberg streaming reader, an overwrite snapshot is **not safe to read incrementally**. The reader cannot tell which rows were added, which were removed, and which were rewritten in place. Flink's Iceberg source connector explicitly rejects overwrite snapshots in streaming mode and will throw an error or skip them entirely. This means you cannot point a Flink streaming reader at a Spark-written silver table and get a meaningful changelog.

### Flink — `append` snapshots with equality deletes

When Flink writes to an Iceberg table with `upsert-enabled=true` and a `PRIMARY KEY`, it uses the `EqualityDeltaWriter`. This produces:

- **New data files** for inserted or updated rows (appended to the table)
- **Equality delete files** that mark which previous rows are superseded (identified by primary key)

The resulting snapshot has `operation = append` — it only adds files, never rewrites them. Existing files are untouched.

This is the key difference. An append snapshot has a clear, unambiguous changelog semantics: here are the new rows, here are the rows to delete. A downstream Flink streaming reader can consume these snapshots one at a time, derive `+I` (insert), `+U`/`-U` (update before/after), and `-D` (delete) records, and maintain correct streaming aggregations.

### Why this matters end-to-end

```
Kafka → Flink bronze (append) → Flink silver (append + equality deletes) → Flink gold (streaming read)
```

This entire pipeline is streaming, continuous, and exactly-once. Each layer reads the previous layer as a live changelog. The gold table (`item_category_count`) reflects the current count of items per category, updated continuously as CDC events flow through from Postgres.

The equivalent Spark pipeline:

```
Kafka → Spark bronze (append) → Spark silver (overwrite) → ??? gold (cannot stream-read silver)
```

Spark can write to silver but nothing can stream-read from it as a changelog. Gold must be computed as a batch job on a schedule, introducing latency and losing the continuous update property.

---

## The Silver → Gold Pipeline in Detail

### Silver table (Flink upsert write)

The silver table `item_attributes_flink_v2` is written by a Flink SQL job that:

1. Reads from the bronze Iceberg table as a streaming source (`starting-strategy = TABLE_SCAN_THEN_INCREMENTAL`)
2. Applies a `LAST_VALUE` / `MAX` aggregation grouped by `item_id` to maintain the latest state per item (SCD Type 1)
3. Writes to silver with `upsert-enabled=true`, using the `EqualityDeltaWriter`

Each checkpoint (every 10 seconds) produces a new append snapshot on the silver table containing the latest upserted rows and equality delete files for any rows that changed.

### Gold table (Flink streaming read)

The gold job `gold_item_category_count_flink_v2` reads from the silver table as a streaming source with a `PRIMARY KEY` declared. This tells Flink the source is an upsert stream — updates arrive as retract-then-insert pairs (`-U` / `+U`). Flink's `GroupAggregate` operator maintains a running `COUNT(item_id)` per category in RocksDB state, correctly incrementing and decrementing as items change category or are deleted.

The result is a gold table that is always current — not a snapshot from the last batch run.

---

## Merge-on-Read vs Copy-on-Write at Scale

### The write mode decision

All Flink silver and gold tables in this project use merge-on-read (MoR). This is set via `ALTER TABLE` after creation (Flink SQL DDL does not support `TBLPROPERTIES` syntax — that is Spark SQL only).

MoR is the right choice for CDC-driven tables because:

- **CDC workloads have a high update ratio.** A single item may be updated dozens of times per hour. With copy-on-write (CoW), each update rewrites the entire Parquet file containing that item's row — write amplification is proportional to file size, not change volume.
- **Flink already writes in MoR style.** The `EqualityDeltaWriter` always produces append + equality-delete files regardless of the table property. Setting MoR explicitly ensures that when Spark reads or compacts these tables, it uses the same strategy rather than defaulting to CoW and triggering expensive full file rewrites.
- **Write latency stays predictable.** MoR write cost is proportional to the change volume, not the table size. As the table grows to millions of rows, write performance is unaffected.

### The compaction tradeoff

MoR shifts cost from writes to reads. As equality delete files accumulate, every read must open them and apply them against data files — effectively a join on every scan. Without compaction this degrades over time.

The operational pattern is:
- **MoR for all CDC tables** — cheap continuous writes
- **Frequent compaction** — collapse equality deletes into clean data files periodically

This project runs a Spark compactor every 10 minutes that calls `rewrite_data_files` and `rewrite_position_delete_files`. At low data volumes this is conservative; at higher volumes the compaction interval should be tuned to keep delete file counts below ~10 per partition.

---

## Technical Challenges Solved

Setting up Flink to write and read Iceberg tables in a Docker-based local environment involved several non-obvious issues worth documenting.

### JAR conflicts — classloader isolation

The `iceberg-flink-runtime` JAR is a fat JAR that bundles Iceberg core classes. Adding `iceberg-aws-bundle` to `/opt/flink/lib/` alongside it causes a `ClassCastException` because the same Iceberg classes (`RESTCatalog`, `Catalog`) get loaded by two different classloaders — Flink's `ChildFirstClassLoader` and the app classloader. The fix is to include only `iceberg-flink-runtime` in `lib/` and let it provide all Iceberg + AWS SDK classes. Similarly, passing the runtime JAR via `--jar` on the `sql-client.sh` command line when it is already in `lib/` causes the same duplication.

### DDL cannot be shared between Spark and Flink

Spark SQL uses `USING iceberg` and `TBLPROPERTIES (...)` syntax. Flink SQL uses `PRIMARY KEY ... NOT ENFORCED` and does not support `TBLPROPERTIES`. These two dialects are incompatible in the same SQL file. The solution is to maintain two DDL files:

- `create_tables.sql` — executed by the Spark `ddl-init` container, creates all `_spark` tables and namespaces
- `create_tables_flink.sql` — executed by `flink-submitter` before job submission, creates all `_flink` tables with correct primary keys and then applies MoR properties via `ALTER TABLE`

### Connector options: `catalog-database` not `database-name`

When defining a Flink `CREATE TEMPORARY TABLE` with `'connector' = 'iceberg'`, the options to specify which catalog table to read are `catalog-database` and `catalog-table`. Using `database-name` and `table-name` (documented in some older versions) results in the table being silently resolved against `default_database` instead, producing zero records with no error.

### Startup strategy for streaming reads on existing data

By default, the Flink Iceberg streaming source starts from the latest snapshot — it only reads new data committed after the job starts. To read existing data first and then continue streaming, set:

```sql
'starting-strategy' = 'TABLE_SCAN_THEN_INCREMENTAL'
```

This performs a full table scan of the current snapshot first, then switches to incremental streaming from that point forward.

### JobManager RPC address with custom entrypoint

The `flink-submitter` container uses a custom `entrypoint` (`/bin/bash submit.sh`) which bypasses the standard Flink Docker entrypoint that normally processes the `FLINK_PROPERTIES` environment variable into `flink-conf.yaml`. The base config has `jobmanager.rpc.address: localhost` and `rest.address: 0.0.0.0` — both wrong for a container that needs to connect to a remote JobManager. The fix is to patch both values in `flink-conf.yaml` at container startup using `sed` before invoking `sql-client.sh`.

### Task slots

Each Flink SQL `INSERT INTO` statement submitted via `sql-client.sh` becomes a separate Flink job consuming one task slot. With 5 jobs (bronze, silver v1, silver v2, gold v1, gold v2), the TaskManager needs at least 5 slots. The default of 4 causes the fifth job to enter a `RESTARTING` loop with `NoResourceAvailableException`.

---

## Architecture Summary

```
Postgres (CDC source)
    │
    ▼ Debezium (Kafka Connect)
Kafka topics
    │
    ├─── Flink bronze job ──────────────────► bronze.item_attributes_flink   (append, no PK)
    │                                              │
    │                                              ▼ Flink silver v2 job (LAST_VALUE agg, upsert)
    │                                        silver.item_attributes_flink_v2  (append + eq-delete, PK=item_id)
    │                                              │
    │                                              ▼ Flink gold v2 job (streaming GROUP BY)
    │                                        gold.item_category_count_flink_v2 (append + eq-delete, PK=category)
    │
    └─── Spark bronze job ──────────────────► bronze.item_attributes_spark    (append, partitioned)
                                                   │
                                                   ▼ Spark silver job (foreachBatch + MERGE INTO)
                                             silver.item_attributes_spark     (overwrite snapshots, MoR)
                                                   │
                                                   ▼ Spark gold job (foreachBatch + GROUP BY)
                                             gold.item_category_count_spark   (batch materialisation)
```

The Flink path is fully streaming end-to-end. The Spark path requires a batch-style micro-batch trigger at every layer and cannot maintain a streaming gold table from the silver layer.

---

## Key Takeaways

1. **Flink writes append snapshots; Spark writes overwrite snapshots.** This single difference determines whether a table can be stream-read downstream.

2. **`PRIMARY KEY NOT ENFORCED` is load-bearing in Flink.** It is not just metadata — it tells the Flink Iceberg connector to use the `EqualityDeltaWriter` (producing append snapshots) and tells downstream readers that the stream has upsert semantics, enabling retraction-based aggregations.

3. **MoR + compaction is the right operational pattern for CDC tables.** MoR keeps write latency flat as table size grows; compaction keeps read latency flat as delete files accumulate. The two should be tuned together based on update rate and read SLA.

4. **The Flink Docker setup has several non-obvious JAR and config requirements.** Most streaming Iceberg tutorials assume a cluster environment; getting the same behaviour in a local Docker setup requires understanding classloader isolation, entrypoint processing, and connector option naming differences between versions.
