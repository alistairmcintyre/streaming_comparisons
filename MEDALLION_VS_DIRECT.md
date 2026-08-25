# Medallion (bronze-as-source) vs CDC-Direct-to-Silver

**Question:** for a stream-readable silver with hard deletes, should silver read
an append **bronze table** as its source, or consume the **CDC changelog** directly?

**It's not medallion vs no-medallion.** The real axis is whether silver *reads
bronze*, or silver reads the changelog while bronze is a parallel raw archive.

## The scale default: fan-out
```
CDC stream ─┬─► bronze append table   (raw archive / replay)
            └─► silver upsert table   (computed directly from the changelog)
```
Same source feeds both; silver's lineage is the changelog, **not** the bronze table.
- Keeps a raw layer for replay, audit, schema-recovery, Kafka-retention decoupling.
- Silver gets native `-D` **hard deletes**, lower latency, no op-column reconstruction.

## When reading bronze as silver's source is justified
- **Short CDC retention** — bronze is the only replayable source of truth.
- **Changelog-preserving raw format** (Paimon changelog tables, Hudi) — bronze-as-
  source keeps RowKinds, so deletes survive the hop.
- **Batch / Spark-`MERGE` shops** — medallion is canonical there; deletes handled by
  MERGE, no streaming read required.

## Why pure SQL can't do through-bronze hard deletes
An append source arrives as `+I` only; SQL cannot manufacture a `-D`. Only a
CDC-format source (`debezium-json`), Paimon's `rowkind.field`, or the DataStream
API can emit deletes. So *through-bronze + hard-delete + pure SQL* is impossible —
you get soft tombstones instead.

## Thesis (for the article)
Strict bronze-as-source is what **manufactures the pain**: it dead-ends Spark into
overwrite snapshots (kills the streaming read) and forces delete-reconstruction
(`rowkind.field`, the sequence-field bug, the DataStream detour). Streaming-first
architectures sidestep all of it by consuming the changelog directly. "Medallion"
is a Databricks framing — a useful principle, not a law.

## Company practice
Streaming-first orgs (large Flink + Iceberg users) lean changelog-direct on the
live path while retaining raw landing zones. *Reasoning from public tech profiles,
not verified internal pipelines.*
