# Schema Evolution Options

How a new source field (e.g. a new `item_attributes` attribute) flows through to
silver automatically, per stack. Reference for later implementation — not yet built.

## The one insight

In every hand-written job the real blocker is the **fixed parse at ingest**
(`from_json(value, FIXED_SCHEMA)` in Spark, or a declared source table in Flink
SQL). A field not in that schema is silently dropped and never reaches any table —
so the table format's evolution support is irrelevant until the parse is fixed.
Everything below is about removing that hardcoded parse.

## Three approaches (ranked)

1. **Purpose-built CDC pipeline tool — least hardcoded, recommended.** A
   declarative ingestion layer that reads the evolving schema (usually from a
   schema registry) and propagates source DDL to the sink automatically. One per
   format (see table). No hand-maintained schema, no DataStream code.
2. **Raw-JSON `payload` column — SQL-friendly middle ground.** Keep a
   `payload STRING` (raw JSON) column alongside typed columns. New fields are
   always captured (zero schema change); promote to a typed column on demand
   (query-time JSON extraction, or a deliberate DDL edit). Never silently lose a
   field; not *automatic* typing.
3. **Hand-rolled DataStream (Flink) / custom Spark — last resort.** Parse to a
   generic record, detect new fields, call the table's `addColumn`, remap. You are
   building a schema-evolution engine that options 1–2 give for free. Only for
   transformation logic that can't be expressed declaratively.

## Per-stack

| Stack | Auto-evolution tool (option 1) | Table-side / write-side notes |
|-------|-------------------------------|-------------------------------|
| **Flink SQL — Iceberg** | **Flink CDC pipeline** connector (Iceberg sink, since Flink CDC 3.4.0, May 2025). `schema.change.behavior = evolve \| try_evolve \| lenient \| ignore \| exception` | Plain Flink SQL is **static**: new field ignored unless declared → edit DDL + restart. |
| **Flink SQL — Paimon** | **Flink CDC pipeline** (Paimon sink) or **Paimon CDC sync actions** (`kafka_sync_table` / `mysql_sync_table`) | Paimon auto-adds columns; cannot rename table / drop column (ignored); rename-column becomes add. |
| **Spark — Hudi** | **Hudi Streamer** (ex-DeltaStreamer) + `SchemaRegistryProvider` | Bronze `from_json` is the blocker. Bronze→silver read is dynamic (carries table schema). DataSource `upsert` write carries all DataFrame columns (no enumerated MERGE list) and **implicitly adds columns**; `hoodie.datasource.write.reconcile.schema=true` + `hoodie.schema.on.read.enable=true` for full evolution. Needs Hudi > 0.11, Spark > 3.1. |
| **Spark — Delta** | **Delta**: `spark.databricks.delta.schema.autoMerge.enabled=true` (auto-evolve on MERGE) or `.option("mergeSchema","true")` | Still needs the new field in the DataFrame first (fix `from_json`). Enumerated MERGE column list is a blocker unless using `UPDATE SET *` / `INSERT *`. |
| **Spark — Iceberg** | (no Spark-native CDC streamer) — use `MERGE INTO ... *` + Iceberg `ALTER TABLE ADD COLUMN`, or ingest via Flink CDC | Iceberg supports full schema evolution at the table level; the Spark job's enumerated MERGE is the blocker. |

## What our hand-written jobs need (additive columns)

- **All stacks:** fix the ingest parse — Avro + Schema Registry (`from_avro` /
  Flink registry format), OR a raw-JSON `payload` column. This is the main work.
- **Hudi silver:** already evolution-friendly on write (DataSource upsert). Just
  set `reconcile.schema` (+ `schema.on.read.enable` for full evolution).
- **Paimon / Iceberg / Delta silver:** the enumerated `MERGE INTO` column list must
  become wildcard (`UPDATE SET *` / `INSERT *`) or move to the format's sync tool.

## Caveats

- **Additive is the easy case** — new column back-filled null for old rows; all
  tools handle it. **Drop / rename / type-change** need the format's full-evolution
  mode + explicit DDL and have reader-support caveats.
- **Athena wrinkle (Objective 3):** Paimon/Hudi/Delta are exposed to Athena as
  Iceberg (compat mode / XTable). An evolved column must also propagate through
  that conversion layer — verify end-to-end, not just on the native table.
- **Keys stay fixed:** record key (`item_id`) and precombine/sequence field must
  not change under evolution.
- **Delete path unaffected:** deletes need only the key, so schema evolution does
  not interact with the delete handling.

## Sources
- [Flink CDC 3.4.0 release](https://flink.apache.org/2025/05/16/apache-flink-cdc-3.4.0-release-announcement/) ·
  [Flink CDC schema evolution](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/core-concept/schema-evolution/) ·
  [Iceberg pipeline connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/iceberg/)
- [Paimon CDC ingestion](https://paimon.apache.org/docs/master/cdc-ingestion/kafka-cdc/)
- [Hudi schema evolution](https://hudi.apache.org/docs/schema_evolution/) ·
  [Hudi RFC-33](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+33++Hudi+supports+more+comprehensive+Schema+Evolution)
