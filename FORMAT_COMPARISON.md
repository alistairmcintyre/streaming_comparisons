# Table Format Comparison — Iceberg vs Delta vs Hudi vs Paimon

> **These are architecture-derived *relative* ratings (★ = weak … ★★★★★ = best-in-class),
> not benchmarks.** Absolute throughput depends on engine, tuning, file sizes, key skew,
> and version. Use this for *design direction* (which format suits which workload), and
> the "why" column, not for capacity planning. Ratings reflect stable releases as of 2026.

## Architecture in one line

| Format | Core design | Update model |
|---|---|---|
| **Iceberg** | Snapshot + manifest metadata tree over immutable data files | Copy-on-write (rewrite files) or MoR (position/equality delete files) |
| **Delta** | Transaction log (`_delta_log`) over data files + deletion vectors | CoW MERGE, or DVs for MoR-style deletes/updates |
| **Hudi** | Timeline + **record-level index**; CoW or MoR table types | Index locates the file for a key → update in place (CoW) or log (MoR) |
| **Paimon** | **LSM-tree** on the lake (sorted runs, levels, merge on compaction) | Append to L0, background compaction merges — built for high-frequency upsert |

## A. Maintenance mechanisms — what each format gives you

| Mechanism | Iceberg | Delta | Hudi | Paimon |
|---|---|---|---|---|
| **Inline write-sizing** | ❌ writes files as produced | ✅ `optimizeWrite` (bin-pack at write) | ✅ file sizing on write | ✅ LSM flush sizing |
| **Auto / in-writer compaction** | ❌ manual only | ✅ `autoCompact` (post-commit, in-writer) | ✅ table service (inline or async) | ✅ LSM compaction (managed) |
| **Manual / scheduled compaction** | `rewrite_data_files`, `rewrite_manifests`, `rewrite_position_deletes` | `OPTIMIZE` (+ Z-order / Liquid Clustering) | compaction + clustering ops | dedicated compaction job |
| **GC / storage reclaim** | `expire_snapshots` + `remove_orphan_files` | `VACUUM` (required; no auto in OSS) | cleaning (table service) | snapshot expiration |
| **Streaming changelog / CDC** | ⚠️ upsert → overwrite snapshots, **not** stream-readable by Flink | ✅ CDF (`readChangeFeed`) | ✅ incremental query + CDC (before/after) | ✅ native `changelog-producer` |
| **Clustering / skipping** | sort order, **hidden partitioning** (+ evolution) | Z-order, **Liquid Clustering** | clustering, **record index** | LSM sort key, bucketing |

**Key takeaway:** Hudi and Paimon bake compaction/cleaning into the engine as managed services; Delta gives in-writer options (`optimizeWrite`/`autoCompact`) but still needs scheduled `VACUUM`; Iceberg maintenance is all explicit jobs. **All four still need a GC step** — compaction never removes dead files, only reorganises live ones.

## B. Rough relative performance

| Dimension | Iceberg | Delta | Hudi | Paimon | Why |
|---|:---:|:---:|:---:|:---:|---|
| **Append write** | ★★★★ | ★★★★ | ★★★ | ★★★★ | Hudi's default path carries index/precombine overhead (use `bulk_insert` for pure append) |
| **Upsert write (high-update / streaming)** | ★★ | ★★★ | ★★★★ | ★★★★★ | Iceberg CoW rewrites whole files (worst); Paimon LSM appends to L0 (best); Hudi index+MoR-log strong; Delta CoW/DV + optimizeWrite shuffle |
| **Compaction (efficiency + managed)** | ★★★ | ★★★★ | ★★★★★ | ★★★★★ | Hudi/Paimon native + managed; Delta in-writer; Iceberg manual procedures only |
| **Read — point / current-view (latest per key)** | ★★★ | ★★★ | ★★★★ | ★★★★ | Hudi record index + Paimon LSM key structure give keyed lookups; Iceberg/Delta scan+filter (no key index) |
| **Scan — large analytical batch** | ★★★★★ | ★★★★ | ★★★ | ★★★ | Iceberg's manifest + partition + column-stats pruning is best; PK/MoR tables (Hudi/Paimon) pay a merge cost on snapshot reads |
| **Partitioning** | ★★★★★ | ★★★★ | ★★★ | ★★★★ | Iceberg **hidden partitioning + partition evolution** is class-leading; Delta ★★★★ *via Liquid Clustering* (plain Hive partitioning ★★★); Hudi global-index cost if partition not key-derivable |
| **Indexing** | ★★★ | ★★★ | ★★★★★ | ★★★★ | Hudi **record-level index** (bloom/bucket/HBase/record_index) is the differentiator; Paimon LSM sort key + bucket; Delta/Iceberg = file stats + Z-order/sort + optional bloom, no record index |
| **Streaming changelog (silver→gold stream-read)** | ★★ | ★★★★ | ★★★★ | ★★★★★ | Paimon native (Flink Table Store heritage); Delta CDF + Hudi incremental are first-class; Iceberg upsert isn't stream-readable as a changelog (v3 DV + Flink→DV work is *emerging*) |
| **GC burden (lower = more managed)** | ★★★ | ★★★ | ★★★★ | ★★★★ | Hudi cleaning / Paimon snapshot-expiry run as services; Delta VACUUM + Iceberg expire/orphan are separate scheduled jobs |
| **Engine breadth** | ★★★★★ | ★★★ | ★★★★ | ★★★ | Iceberg broadest (Spark/Flink/Trino/Athena/BigQuery…); Delta Spark-first; Hudi Spark+Flink; Paimon Flink-first (Spark improving) |

## Pick by workload

- **High-volume, high-update streaming CDC → current-view + streaming gold** (your KnowBe4 case): **Paimon + Flink** (LSM absorbs the churn, native changelog for incremental gold) or **Hudi MoR** (record index, mature table services). Delta+CDF is the strong Spark-native option.
- **Append-heavy analytical lake, broad engine access, Athena/Trino/BigQuery consumers:** **Iceberg** — best scan pruning, hidden partitioning, widest reader support. (Just don't ask its upsert silver to be Flink-stream-readable.)
- **Databricks / Spark-centric shop wanting one format for batch + streaming:** **Delta** — CDF for streaming reads, optimizeWrite/autoCompact/Liquid Clustering for layout, mature Spark tooling.
- **Frequent point lookups by key on a huge upsert table:** **Hudi** (record index) or **Paimon** (LSM key).

## Cross-cutting rules (apply regardless of format)

1. **Never full-scan silver to build gold** — incremental view maintenance over the changelog (see `STREAMING_DESIGN_PRINCIPLES.md` §1).
2. **Compaction ≠ GC.** In-writer compaction (optimizeWrite/autoCompact/LSM/table-services) avoids concurrent-writer conflicts, but you still need a GC step (VACUUM / expire_snapshots / cleaning / snapshot-expiry) — and it *increases* dead files, so GC becomes more necessary.
3. **Partition/cluster by the entity you filter+aggregate on, on an immutable field** — never a mutable one; watch reader-lag vs GC retention.
