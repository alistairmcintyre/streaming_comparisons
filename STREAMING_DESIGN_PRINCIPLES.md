# Streaming Lakehouse Design Principles

Design decisions for building **silver → gold** streaming pipelines over
high-volume, high-update CDC data (payments, trades, customer profiles). Written
from the findings in this project, but the principles are format-agnostic.

---

## 1. Never full-scan silver to build gold — the cardinal rule

**The single most important silver-pipeline consideration.** A gold aggregation
that re-reads the whole silver table on every micro-batch is `O(rows)` per
trigger. At millions of rows it's wasteful; at billions it is simply dead.

```
# ANTI-PATTERN — dies at scale
foreachBatch:
    spark.read(silver).groupBy(key).agg(...)   # scans ALL current rows, every batch
    MERGE into gold
```

Two things people reach for that **do not** fix this:

- **Clustering / partitioning / Z-order / liquid clustering** — these speed up
  *filtered* reads via data skipping. A global aggregation (`GROUP BY country
  COUNT`) has no predicate to skip on: you must touch every current row to count
  it. Clustering helps the *batch/Athena read side*, never a full aggregation.
- **Bigger compute** — scaling the cluster to scan a billion rows every 15s is
  paying to do the wrong thing.

**The rule:** gold must be maintained **incrementally from the change stream** —
work proportional to the number of *changes*, not the table size.

---

## 2. First question for any gold aggregation: does the metric depend on a *mutable* field?

This one decision drives the entire design.

| Metric type | Examples | Approach | Needs current-view silver? |
|---|---|---|---|
| **Immutable-event** | trade count / notional volume / VWAP per instrument per window | Aggregate the **append stream** with event-time **windows + watermark** | **No** — read the append log (bronze) directly |
| **Current-state** | open exposure per counterparty, count/sum by *current* status, current balance, active customers per country | **Retraction-aware incremental view maintenance** over the changelog | Yes |

A large share of analytics is actually the *immutable-event* case — and there the
updates are irrelevant to the measure, so you skip current-view maintenance and
retractions entirely. Only reach for the harder pattern when the metric genuinely
tracks mutable state.

---

## 3. Retraction-aware incremental aggregation (for current-state metrics)

Consume **only the changes** and apply signed deltas to a small running-aggregate
table (which *is* the state):

```
insert, update_postimage   -> +1   (row enters the group)
delete,  update_preimage   -> -1   (row leaves the group)
```

A group-key change (e.g. a customer's country) arrives as `preimage(-1 old)` +
`postimage(+1 new)`, so it nets correctly with **no full recount**. For SUM,
delta = `post.amount - pre.amount`.

Per-format changelog mechanisms:

| Format / engine | Changelog mechanism | Notes |
|---|---|---|
| **Flink** (any PK source) | Native retraction: streaming `GROUP BY` keeps the aggregate in managed (RocksDB) state and emits `-U/+U` automatically | Strongest fit — the engine does it for you |
| **Paimon** | `changelog-producer` (input / lookup / full-compaction) | LSM current-view table *and* a stream-readable changelog |
| **Delta** | Change Data Feed (`readChangeFeed`, `_change_type`) | Must set `delta.enableChangeDataFeed=true` at table creation (not retroactive) |
| **Hudi** | Incremental query (`hoodie.datasource.query.type=incremental`) | Reads commits since an instant |
| **Iceberg** | ⚠️ upsert silver = `overwrite`/equality-delete snapshots — **not** stream-readable by Flink → Iceberg gold must be **batch** (see §7) |

---

## 4. Format & engine choice for a high-update silver (e.g. 30% updates)

Two separate costs at a high update ratio:

- **Maintaining the current-view silver** (the upsert). LSM formats
  (**Paimon**, **Hudi MoR**) absorb high update rates best: append deltas to log
  files, compact in the background. **Delta** MERGE rewrites files (or uses
  deletion vectors) — heavier write-amplification at extreme update rates.
- **Maintaining gold** (the aggregate). **Flink** does retraction-aware
  aggregation natively with bounded, TTL-able per-key state. **Spark** has no
  native retraction aggregation, so you hand-roll the changelog-delta MERGE.

**Recommendation for a payments/trades shop:** Flink + Paimon (or Fluss) —
LSM silver for the update churn, Flink streaming `GROUP BY` for incremental
aggregates. Split immutable-event windowed aggregations (off the append stream)
from current-state retraction aggregations (off the changelog).

---

## 5. Idempotency & correctness

- **Idempotent counters.** `foreachBatch` is at-least-once; running counters
  double-count on retry. Stamp each write with an app id + the batch id
  (Delta: `txnAppId` / `txnVersion`) so a replayed batch is a no-op. Flink's
  checkpointed state gives exactly-once for free.
- **Out-of-order corrections** (trades get amended late). Use event-time +
  **watermark** and a **sequence field** (e.g. Debezium `ts_ms`) for
  last-writer-wins in silver — never `source_updated_at`, which is null on
  deletes.
- **Drift backstop.** Run a periodic **batch reconciliation** (nightly full
  recompute) to correct any incremental drift — the "kappa + reconcile" pattern.
  Bound normal lateness with the watermark; the reconcile catches the long tail.

---

## 6. Clustering / partitioning: a read-side tool only

Use them for the **batch/Athena read side** — skip data for
`WHERE counterparty=… AND trade_date=…`. Cluster/partition by the **query
predicate** (date, instrument), *not* by the streaming aggregation key. **Never
partition silver by a mutable field** (status, country): an update moves the row
between partitions (delete+insert churn).

---

## 7. The Iceberg caveat (why this project uses Paimon/Hudi/Delta for streaming gold)

A Flink upsert silver (or Spark `MERGE`) on Iceberg writes **`overwrite`
snapshots with equality/positional deletes** — every row, inserts included.
Flink's Iceberg streaming source **cannot emit a changelog from those**, so a
downstream streaming gold reads nothing (verified: gold empty after 300s). Under
Iceberg you therefore either accept a **batch** gold, or keep silver as an
**append-only changelog** and derive the current view on read.

The streaming-native formats (**Paimon** changelog, **Hudi** incremental, **Delta**
CDF) exist precisely to give you a current-view table that is *also*
stream-readable as a changelog — which is what makes §3's incremental gold
possible.

---

## Concrete example (this repo): active customers per country

`jobs/spark-delta/gold_customers_per_country.py` — Delta CDF incremental gold:
reads `silver` via `readChangeFeed` (only the changes), maps `_change_type` to
`+/-1`, nets per country, and MERGEs the deltas into the tiny gold table with
`(txnAppId, txnVersion)` idempotency. `maxFilesPerTrigger` bounds each batch so
even the one-time catch-up from version 0 stays flat in memory. Work is
proportional to changes, not to silver's size.

The equivalent in Flink (`jobs/flink-paimon/gold_customers_per_country.sql`) is a
one-liner — `SELECT country, COUNT(*) FROM silver GROUP BY country` over the
Paimon changelog — because Flink maintains the retraction aggregate natively.
