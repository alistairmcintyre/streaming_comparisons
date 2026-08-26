# AWS Testing Objectives

Tracking doc for validating this project's streaming Iceberg findings on AWS.
We will progress through the objectives below and record results in place.

---

## Background

Local testing (see `ARTICLE_NOTES.md`) established the core problem:

- **Spark `foreachBatch` + `MERGE INTO` produces `overwrite` snapshots** on Iceberg
  tables. Overwrite snapshots cannot be consumed by a streaming reader — Flink's
  Iceberg source rejects them — so a Spark-merged silver table is a dead end for
  any downstream streaming consumer.
- **Flink upsert writes (`upsert-enabled=true` + PRIMARY KEY) use the
  `EqualityDeltaWriter`**, producing append + equality-delete snapshots that ARE
  stream-readable downstream.

**Critical requirement (refined 2026-07-25): silver tables must be
stream-readable; gold tables only need batch reads.** Athena is a batch
engine — it scans the table's current snapshot and never reads snapshot
history incrementally — so snapshot operation types (append vs overwrite) are
irrelevant to Athena consumers. Stream-readability matters on the hop that
feeds downstream streaming jobs: reading silver. For the non-Iceberg formats,
that hop can use the format's native changelog mechanism (Paimon streaming
read, Delta CDF, Hudi incremental query); an Iceberg-compat view of those
tables then only has to serve batch consumers, and its open question becomes
**freshness** rather than snapshot semantics.

The next phase is validating that this holds on AWS infrastructure (Glue
catalog, S3, Athena) rather than the local stack (REST catalog, MinIO).

## Target environment

- **AWS account:** `167217327348`
  (member of org `o-jpn3xda2f0`, management account `747461892764`;
  full ARN: `arn:aws:organizations::747461892764:account/o-jpn3xda2f0/167217327348`)
- **Region:** TBD
- **Catalog:** AWS Glue Data Catalog
- **Query engine for validation:** Amazon Athena
- **Object store:** S3 (general purpose bucket; S3 Tables not in scope unless decided otherwise)

---

## Objective 1 — Can Flink stream-read an Iceberg upsert (current-view) silver into gold? → **NO**

**RESOLVED locally 2026-08-02 — CANNOT be done. No AWS run needed** (this is an
engine/format limitation, not environment-specific — it reproduces identically
anywhere).

The original premise here ("inserts and updates produce append snapshots, only
deletes are the open question") was **wrong**. Verified with
`make integration-test STACK=flink-iceberg` + `make snapshots`:

- A Flink **upsert** sink (`upsert-enabled=true` — required for a current-view,
  one-row-per-key silver) writes an equality-delete **+** a data record for
  **every** row, inserts included (it can't know in a streaming write whether the
  key already exists). Any commit that contains delete files is an Iceberg
  `RowDelta`, whose `operation` is **`overwrite`**, never `append`. Even the first
  insert into an empty table committed as `overwrite`. The only `append` snapshots
  on the silver table are **empty checkpoint heartbeats** (zero `added-records`) —
  reassuring to look at, but no rows live there.
- Flink's Iceberg **streaming source cannot emit a changelog from overwrite /
  equality-delete snapshots**, so a downstream gold streaming job reads nothing.
  Confirmed: gold stayed **empty after 300s** with all 5 jobs RUNNING and silver
  fully populated + batch-readable. Not a timing issue. (Corroborated by 2026
  Iceberg community writing on the "equality delete" streaming-read problem;
  deletion vectors + Flink→DV conversion are the emerging, not-yet-here fix.)

**Conclusion:** you cannot have a single Iceberg table that is *both* a stored
current-view (upsert) silver *and* stream-readable into gold under Flink. Pick one:
- **(a)** append-changelog silver → `append` snapshots, stream-readable, but the
  current view is derived on read (gold must dedup latest-per-key). Rejected here:
  we don't want dedup logic in gold.
- **(b)** keep the upsert current-view silver and make **gold batch** (Spark or
  Flink **batch**) to pick up updates/deletes. ← accepted for the Iceberg track.
- **(c)** use a streaming-native format that supports all three at once → Objective 2.

Spark's `MERGE`-into-Iceberg silver has the same shape (overwrite snapshots), so a
Spark Iceberg gold consuming updates/deletes must likewise be **batch**.

**Results:** CANNOT be done — retires this objective's AWS tasks. The real question
becomes Objective 2: which format gives stream-write + current-value-per-ID +
stream-read together.

---

## Objective 2 — Iceberg tables remain format v2 (Athena compatibility)

Athena requires Iceberg **format-version 2** tables. Newer Iceberg runtimes are
introducing v3 features (deletion vectors, row lineage); we must confirm nothing
in our toolchain creates or silently upgrades tables to v3.

**Tasks**
- [ ] Pin `'format-version' = '2'` explicitly in all table DDL
      (`ddl/create_tables.sql`, `ddl/create_tables_flink.sql`).
- [ ] Check the Iceberg runtime versions in the Docker images / AWS runtimes for
      their default format version and any auto-upgrade behaviour.

**Verification**
- [ ] After the pipelines have run (including compaction —
      `jobs/spark/maintenance_compaction.py`), check each table's metadata JSON
      or `SELECT * FROM <table>.metadata_log_entries` / table properties and
      confirm `format-version: 2`.
- [ ] Query every bronze/silver/gold table from Athena (point-in-time SELECT +
      a `FOR TIMESTAMP AS OF` / snapshot query) and confirm success.

**Results:** _(pending)_

---

## Objective 3 — Hudi / Paimon / Delta pipelines producing Iceberg-compatible tables (Glue + Athena)

Determine whether the Spark pipelines in `jobs/spark-hudi/`, `jobs/spark-paimon/`
and `jobs/spark-delta/` can produce tables that (a) register correctly in the
Glue catalog as Iceberg, (b) can be queried by Athena, and (c) stay acceptably
fresh. Per the refined requirement (see Background), the Iceberg-compat view
serves batch consumers only — silver stream-readability is satisfied by each
format's native changelog — so the question is how far the Iceberg view lags
the native table, not whether its snapshots are stream-readable.

Working hypotheses to confirm:

| Format | Mechanism | Freshness of Iceberg view | Notes / open questions |
|--------|-----------|---------------------------|------------------------|
| Paimon | Iceberg compatibility mode (`metadata.iceberg.storage`) | **Compaction-gated for primary-key tables**: the Iceberg view can only expose fully-compacted (highest LSM level) files, since Iceberg readers cannot apply Paimon's level-merge semantics. Append-only tables advance per commit. | Bound staleness with `full-compaction.delta-commits` or `compaction.optimization-interval`; frequent full compaction = write amplification. Check which storage mode syncs to Glue. |
| Delta  | UniForm (`delta.universalFormat.enabledFormats = iceberg`) | **Per-commit, not compaction-gated**: metadata-only async conversion after each Delta commit (shared Parquet data files); lag is typically seconds. | Requires Delta 3.x, column mapping (name mode), IcebergCompat feature, and **deletion vectors disabled** — so merges become copy-on-write file rewrites (cost hit on write, not freshness). Confirm Glue registration path. |
| Hudi   | **No native Iceberg metadata mode known.** | n/a — depends on conversion tool cadence if XTable is used. | Likely needs Apache XTable (incubating) as a conversion layer, or falls back to Athena's native Hudi read support (acceptable now that Athena consumers are batch-only, but it is not an Iceberg table). Investigate and record the conclusion either way. |

**Tasks**
- [ ] Paimon: enable Iceberg compatibility on the silver pipeline; sync to Glue;
      document required properties and compaction settings.
- [ ] Delta: enable UniForm on the silver pipeline; sync to Glue; document
      required properties.
- [ ] Hudi: investigate options (XTable vs none); document the conclusion.
- [ ] Define maintenance per format, on the native side only: Delta `OPTIMIZE` +
      `VACUUM` + log retention; Paimon compaction schedule + snapshot
      expiration (automatic on commit). The Iceberg-compat metadata is derived
      and read-only — never run Iceberg maintenance procedures
      (`expire_snapshots`, `rewrite_data_files`, `remove_orphan_files`) against
      it. `jobs/spark/maintenance_compaction.py` applies only to the native
      Iceberg (Flink-written) tables.
- [ ] Multi-writer safety on S3 (maintenance job vs streaming writer):
      - **Delta OSS**: the default S3 LogStore only serializes commits within a
        single driver. A separate maintenance application committing
        concurrently can silently overwrite a `_delta_log` version file and
        corrupt the log. Either configure the multi-cluster
        `S3DynamoDBLogStore` on **all** writers of the table, or run
        `OPTIMIZE`/`VACUUM` from within the writer application.
        **Verified 2026-07-25:** no delta-spark release uses S3 conditional
        writes (put-if-absent) for commits — feature request
        [delta-io/delta#3596](https://github.com/delta-io/delta/issues/3596)
        (Aug 2024) is still open with no linked implementation,
        [#4813](https://github.com/delta-io/delta/issues/4813) (Jun 2025) is
        unanswered, and the current storage docs still mandate
        `S3DynamoDBLogStore` for multi-cluster S3 writes. (delta-rs is the
        implementation that uses S3 conditional puts — Rust/Python writers,
        not Spark.) The project's direction for commit coordination is
        catalog-managed tables (preview since Delta 4.0, Unity
        Catalog-oriented) rather than a conditional-write LogStore — not
        applicable to a Glue-based OSS stack. **Conclusion: DynamoDB LogStore
        on all writers, or keep all writes (incl. maintenance) in a single
        Spark application.**
      - **Hudi**: compaction/cleaning are table services that run inline in the
        writer by default — no separate job required. Async services in a
        separate process require OCC + a lock provider (DynamoDB-based
        provider available).
      - **Paimon**: compaction is inline with writes by default; periodic full
        compaction for Iceberg-view freshness can be driven by the writer
        itself (`full-compaction.delta-commits` /
        `compaction.optimization-interval`). Dedicated compact job with
        `write-only` writers is the supported offload pattern — verify catalog
        lock requirements on S3.

**Verification (per format)**
- [ ] Table visible in Glue catalog with Iceberg metadata.
- [ ] Athena `SELECT` returns correct, current results after ongoing writes.
- [ ] Inspect the generated Iceberg snapshots' `operation` values under
      insert/update/delete traffic — are they stream-readable (append) or
      overwrite?
- [ ] Attempt a Flink streaming read of the Iceberg-compat view of the table;
      record whether it works, and with what freshness/latency (e.g. Paimon
      only advancing Iceberg metadata on compaction).

**Results:** _(pending)_

---

## Objective 4 — Apache Fluss as the streaming storage layer (Flink track)

Evaluate Apache Fluss (incubating) in the Flink pipeline. Fluss is Flink-native
streaming table storage: **Log Tables** (append) and **Primary Key Tables**
(upsert with a true changelog), columnar streaming reads with projection
pushdown, and a **datalake tiering service** (a Flink job) that materializes
tables into a lake format for batch consumers, with "union read" combining the
real-time Fluss tail and the lake history.

Why it is attractive for our requirement:

- **Stream-readability moves to the Fluss layer.** A PK table serves a
  sub-second CDC changelog (`+I`/`-U`/`+U`/`-D`) directly to downstream Flink
  jobs — silver's stream-readability no longer depends on Iceberg snapshot
  semantics at all.
- **Iceberg is demoted to the batch/serving layer** via tiering — Athena
  queries the tiered tables; append-vs-overwrite becomes irrelevant there.
- Potentially collapses the stack: Flink CDC could write Postgres changes
  straight into Fluss PK tables, removing Kafka + Debezium/MSK Connect, and a
  Fluss silver PK table subsumes the bronze→silver hop.

Hypotheses / open questions to verify:

- Paimon was Fluss's first-class lake format; **Iceberg tiering is newer** —
  confirm minimum Fluss version, PK-table→Iceberg tiering support, and what the
  tiered tables look like (format v2, delete files, compaction ownership).
- Glue catalog support in the tiering service; Athena queryability of tiered
  tables.
- **No managed AWS offering** — self-hosted cluster (coordinator + tablet
  servers; ZooKeeper dependency depending on version) on EKS/EC2, S3 as
  remote/tiered storage. Ops cost vs MSK must be weighed.
- Where the tiering service Flink job runs (EMR vs Managed Flink).
- Who maintains the tiered Iceberg tables (tiering service vs separate job).
- **Engine support beyond Flink is minimal.** Flink is the only first-class
  connector; a Spark connector is roadmap-level (verify current status). The
  intended pattern for Spark/Trino/Athena is batch-reading the tiered lake
  tables — no changelog reads, no union read, freshness = tiering lag. If any
  consumer needs Spark *streaming* reads, Fluss does not provide them; that
  consumer would be back to stream-reading the tiered Iceberg table, where
  snapshot semantics matter again (verify what operations the tiering service
  writes).

**Tasks**
- [ ] Stand up a minimal Fluss cluster locally (docker-compose) and model
      bronze/silver as Fluss tables fed by CDC.
- [ ] Enable Iceberg lake tiering; register tiered tables in Glue; query from
      Athena.
- [ ] Point the gold job at the silver PK table's Fluss changelog; verify
      deletes decrement counts correctly.
- [ ] Repeat on AWS (EKS/EC2 + S3) and record ops burden vs MSK.

**Verification**
- [ ] Changelog correctness through insert/update/delete traffic.
- [ ] Freshness of the tiered Iceberg table (lag vs Fluss commit).
- [ ] Athena results match Fluss union-read results.
- [ ] Recovery behaviour: kill/restart tiering service and writer; confirm
      exactly-once tiering (no dupes/gaps in the Iceberg table).

**Results:** _(pending)_

---

## Objective 5 — Serialization format as a measured dimension (JSON vs Avro)

**Why this is an objective and not a footnote.** Every engine currently consumes the
same Debezium **JSON** topic, which makes the engine-vs-engine comparison fair —
serialization is a shared upstream constant, so it cancels out of the relative
ranking. But it does not disappear from the numbers:

- **JSON parse cost is common-mode.** It sits inside every engine's measured latency.
  It does not bias A against B, but it inflates all absolute figures and *compresses
  the gaps between engines* — two engines that differ by 20% in real work can look
  10% apart once both carry the same fixed parsing tax.
- **Payload size is ~3–5× Avro's.** That is more Kafka I/O, more network, more page
  cache, and more broker CPU — none of which is the lake engine under test.
- **It gets worse with rate.** At 1k/s the tax is small. At 10k/30k a materially
  larger share of what we measure is Kafka + JSON parsing rather than Iceberg vs
  Delta vs Paimon vs Fluss. A high-rate run where all four engines look suspiciously
  similar is the signal that common-mode cost is dominating.

**So run it twice and record both.** Rather than treating serialization as a confound
to be argued away, make it the second axis of the matrix:

| | Iceberg | Delta | Paimon | Fluss |
|---|---|---|---|---|
| **JSON** | ✅ current | ✅ | ✅ | ✅ |
| **Avro** | pending | pending | pending | pending |

Holding everything else identical (rate, cluster size, duration, node types), the two
result sets answer two different questions:

1. **Within a format** — the engine ranking, with the format's tax as a constant.
2. **Across formats** — how much of a CDC lakehouse pipeline's latency is actually
   serialization. That is a genuinely useful finding in its own right, and it is
   invisible if only one format is ever measured.

It also tests whether the ranking is *stable* across formats. If Avro reorders the
engines, the JSON ranking was partly an artifact of parsing cost — worth knowing
before drawing conclusions from either set alone.

**Recording:** results are written per run to
`s3://<warehouse>/benchmarks/<run_id>/wire_format=<json|avro>/`, so the two sets stay
distinguishable and directly comparable. The `wire_format` workflow input labels the
run; do not compare across runs that differ in anything else.

**Implementation:** Avro requires a Schema Registry plus a converter swap on Debezium;
`decimal.handling.mode` then becomes `precise`, using Avro's native decimal logical
type instead of the string encoding JSON forces.

Registry choice — **Apicurio, not AWS Glue Schema Registry** (evaluated 2026-08-26).
Glue looks attractive because it is managed, but its Flink integration
(`schema-registry-flink-serde`) is **DataStream-only**: the jar registers no Flink
table factory, so there is no SQL format, and our Fluss/Paimon pipelines are pure SQL.
`flink-avro-confluent-registry` does register `DebeziumAvroFormatFactory`, giving
`'format' = 'debezium-avro-confluent'` directly — and Apicurio speaks the
Confluent-compatible API, so that works against it unchanged. Glue also uses its own
18-byte wire prefix rather than Confluent's 5-byte one.

Spark is the awkward side either way — OSS Spark has no registry integration at all
(`from_avro` takes a literal schema). For a frozen-schema benchmark the pragmatic route
is stripping the 5-byte Confluent prefix and pinning the writer schema; ABRiS is the
fuller option but is Scala-first, which our PySpark jobs would have to reach through
py4j. Note `spark-avro` is not currently in the Spark images at all. See the deploy
to-do list.

**Results:** _(pending — JSON set not yet complete either; Iceberg and Fluss have never
been observed writing data files)_

---

## Decisions & open questions

- [ ] Region for deployment.
- [ ] Flink runtime: Managed Service for Apache Flink vs EMR (see Objective 1).
- [ ] Spark runtime: EMR / EMR Serverless / Glue ETL jobs.
- [ ] Glue catalog access mode for non-AWS engines: native Glue catalog
      integrations vs Glue's Iceberg REST catalog endpoint.
- [ ] CDC source strategy on AWS (live RDS + Debezium vs replaying captured
      events into MSK).
- [ ] Teardown/cost controls: everything here is throwaway test infra — prefer
      serverless/spot where possible, tag resources for cleanup.
- [ ] Delta maintenance concurrency model (see Objective 3 multi-writer bullet):
      1. **`S3DynamoDBLogStore` + separate maintenance job** — decoupled;
         matches the realistic production posture (any real deployment
         eventually has a second writer: backfill, ad-hoc fix, GDPR delete,
         and safety requires the LogStore on ALL writers anyway); trivial
         DynamoDB cost. Logical MERGE/OPTIMIZE conflicts still possible →
         retries, not corruption.
      2. **Sequential in-app maintenance** (call OPTIMIZE/VACUUM between
         micro-batches) — zero conflicts, no DynamoDB, but ingestion pauses
         for the duration of each OPTIMIZE; source backlog absorbs the pause.
      3. **Async maintenance thread inside the writer app** — no pause, no
         DynamoDB, still corruption-safe (the single-JVM LogStore serializes
         commits within one driver), but MERGE/OPTIMIZE can logically
         conflict → retry handling needed.
      Leaning **option 1** for the AWS tests, as it is the posture a
      multi-team production setup needs regardless.

## Cost estimate (2026-07-26, us-east-1 on-demand, ±15%)

Scope: generator + Kafka + 4 stacks (Flink/Iceberg, Spark/Delta, Spark/Hudi,
Spark/Paimon) × bronze/silver/gold = 12 streaming jobs + maintenance, with
Grafana observability. At 50 msg/s, data volume is negligible (~2 GB/day raw);
cost is dominated by always-on compute and S3 request rates from streaming
commits.

**Self-managed path** (single r7g.2xlarge running the docker-compose topology
against S3 + Glue; Prometheus + Grafana self-hosted):
- 24/7 on-demand: **~$350–400/mo** (box ~$315, S3 requests $20–60, EBS ~$8,
  storage/Athena/DDB/IPv4 ~$10–20)
- 24/7 spot: **~$140–180/mo**; 8×5 stop/start: **~$90–120/mo**
- Stage 1 (box stopped except sessions): **~$15–25/mo + ~$0.55/hr while testing**

**Managed path**: MSK provisioned ~$80 + Managed Flink (1 app, statement set,
2 KPU) ~$165 (3 apps ~$480) + single-node EMR ~$390 + Managed Grafana $9 +
misc $30–70 ≈ **$700–1,000+/mo**. EMR Serverless / Glue streaming are
uneconomical for 24/7 jobs. MSK Serverless (~$550/mo floor) avoided.

Cost levers (impact order): stop/start schedule; spot; **trigger/checkpoint
intervals** (10 s commits × 12 jobs ≈ 3M commits/mo → S3 request charges can
exceed $100/mo; 60 s cuts ~6×); no NAT gateway (public subnet + S3 gateway
endpoint); no custom CloudWatch metrics/logs (Prometheus local).

Generator: EC2 nano or Lambda both <$5/mo — co-locate a script on the infra
box; same script serves stage-1 manual publishing.

Dashboard plan: Grafana + Prometheus (Flink Prometheus reporter; Spark
listener/JMX) for job metrics; per-stack end-to-end latency panels computed as
`commit_ts − event_ts`; gold-table preview panels via Athena datasource
(note Athena 10 MB/query minimum → keep auto-refresh ≥1–5 min, ~$1–9/mo).
Doubles as the Objective 3 freshness measurement.

### Chosen model (2026-07-26): session-based runs, production-grade managed services

Usage pattern: 30–60 min sessions, not 24/7. Production-representative stack
(user wants prod-like infra; Apple's known stack is Flink/Spark/Iceberg on
Kubernetes). EKS options for session use (corrected 2026-07-26 — sessions on
k8s ARE viable):
- **Persistent control plane + scale-to-zero nodes** (Karpenter/Auto Mode):
  $73/mo standing, ~3–5 min session start, per-session node cost ≈ what
  Managed Flink + EMR would cost. Total ≈ $110–140/mo. Closest to Apple-style
  prod; how real dev/test EKS is run.
- **Ephemeral cluster per session**: $0 standing, but ~20–25 min spin-up
  (control plane + nodes + operators + image pulls) and the largest
  automation surface. ≈ $40–60/mo.
- **Managed services path** (MSK Serverless + Managed Flink + EMR): lowest
  friction, ≈ $40–75/mo; architecture under test is identical.

**Decision (2026-07-26): testing the Flink Kubernetes Operator on EKS is a
primary goal; EPHEMERAL cluster per session** (no standing control plane —
$73/mo not worth it for this usage). Cluster + MSK Serverless created by
`make up` (~15 min), **auto-teardown ~60–90 min after readiness**. Spark jobs
run on the same cluster via the Spark Operator (no EMR).

**Spark version + operator decision (2026-07-29): migrate the Spark stack to
Spark 4.0.x and use the official Apache Spark K8s Operator 1.0 GA.**
Verified on Maven Central that all four formats ship Spark 4.0 / Scala 2.13
builds, so there is no compatibility reason to stay on 3.5:
- Iceberg `iceberg-spark-runtime-4.0_2.13` 1.11.0
- Delta `delta-spark_2.13` 4.3.1
- Hudi `hudi-spark4.0-bundle_2.13` 1.2.0
- Paimon `paimon-spark-4.0` 1.3.2

Target **Spark 4.0.x specifically** (not 4.1 — format 4.1 bundles are not
uniformly published yet; the Apache operator 1.0 supports 4.0). Using the
official Apache Spark operator pairs with the official Flink K8s Operator for a
consistent "official operators" story; **Kubeflow Spark Operator v2.x is the
low-risk fallback** (also supports Spark 4). Streaming jobs run as
`SparkApplication` with `restartPolicy: Always` + S3 checkpoints (resume across
sessions).

Migration checklist (mostly mechanical):
- Scala `_2.12` → `_2.13` on every JAR coordinate; `SCALA_VERSION=2.13` in the
  four Spark Dockerfiles.
- Base image `apache/spark:3.5.6` → `apache/spark:4.0.x`; `pyspark==4.0.x`.
- Bump format JARs to the versions above.
- Kafka connector `spark-sql-kafka-0-10_2.13` for Spark 4.0.x (+ token-provider,
  commons-pool2).
- S3: Spark 4 bundles Hadoop 3.4.x → align `hadoop-aws` to 3.4.x and switch to
  the AWS SDK v2 bundle.
- **Verify ANSI SQL mode (ON by default in Spark 4)** doesn't break casts in the
  jobs — the one behavioral risk.
Cost: **~$2–3/session + ~$5–15/mo baseline ≈ $40–60/mo** at 3–4 sessions/wk.

Making 15-min spin-up and ephemerality work:
- Persist between sessions (free/cheap, slow to create): VPC, IAM roles,
  ECR images, node AMI / EBS image-cache snapshot, S3 (tables, checkpoints —
  Flink/Spark jobs resume from S3 checkpoints across sessions), Glue, DDB.
- Recreated each session by automation: EKS control plane (~10–12 min),
  nodes, operator helm releases + CRDs, FlinkDeployment/SparkApplication CRs,
  MSK Serverless + topics (parallel with cluster creation).
- Slim arm64-only images + node-local image cache (custom AMI or Karpenter
  `EC2NodeClass` EBS snapshot) → pod starts in seconds. (EKS Auto Mode
  disallows custom AMIs — plain Karpenter or managed node groups.)
- Grafana dashboards provisioned as code (configmaps in repo) — the cluster
  is disposable, dashboards must not live only in it. Prometheus history is
  per-session (lost at teardown; acceptable — gold/silver tables + snapshot
  history persist in S3 for cross-session comparison).
- Auto-teardown mechanism: one-shot EventBridge Scheduler created by
  `make up` → triggers CodeBuild running `terraform destroy -auto-approve`
  (cloud-side, survives laptop closing). Teardown deletes k8s CRs/Services
  first so operator-created ENIs/LBs are cleaned before cluster deletion.
  AWS Budgets alarm (~$25/mo) as final backstop.
- Session flow: stage 1 — manual publish script → verify gold via Athena/
  Grafana; stage 2 — generator at 50 msg/s → watch delay panels.

### Phase plan (2026-07-26)

1. **Phase 1**: item_attributes pipelines only (drop item_inventory /
   item_sales from AWS scope) — Flink/Iceberg + Spark stacks (Iceberg, Delta,
   Hudi, Paimon), fed by manual inserts into in-cluster Postgres via
   Debezium.
2. **Phase 2**: enable generator at ~50 msg/s; compare per-stack processing
   delay + gold freshness in Grafana.
3. **Optional later**: add Flink/Paimon pipeline (native pairing, low
   effort). **Dropped**: Flink/Hudi (less native, high tuning cost — decided
   2026-07-26); Flink/Delta (not feasible — Flink Delta connector is
   append-only, no upserts).

Per session (~$2–4): Managed Flink apps stopped between sessions
($0.22–0.66/session, state snapshotted); EMR recreated per session (~$0.47/hr,
~10 min spin-up); **MSK Serverless created/deleted per session** (~$1.20;
provisioned MSK can't stop, bills $80/mo idle, and takes 20–40 min to create —
avoid). CDC source (revised 2026-07-26): **Postgres + Debezium Connect as
in-cluster pods** — mirrors docker-compose, zero marginal cost, ephemeral is
fine (source rows inserted manually per session; durable state is in S3).
Connect worker needs `aws-msk-iam-auth` for MSK Serverless;
`snapshot.mode=initial` captures pre-connector inserts.

Persistent baseline (~$10–20/mo): S3 tables/checkpoints (state survives
sessions), Glue, DynamoDB, Grafana (t4g.micro stopped between sessions or
Managed $9/mo). **At 3–4 sessions/week: ~$40–75/mo all-in.**

Session ops: `make up` / `make down` via IaC (~10–15 min spin-up);
EventBridge-scheduled teardown backstop; AWS Budgets alarm ~$25/mo (idle
stack ≈ $40/day — teardown discipline is the main cost risk).

Short-session caveat: set aggressive maintenance intervals (Paimon full
compaction every ~5 commits, Delta OPTIMIZE ~10 min) so freshness/compaction
behaviour spans multiple cycles within one session.

## Validation findings (local integration test, 2026-07-30)

Ran a local end-to-end integration test of the **spark-paimon** customers pipeline
(10 inserts, 2 country updates, 1 GDPR delete → bronze → silver). It caught two
real bugs that would have shipped to AWS and broken **every Spark stack**
identically — cheap to fix locally, expensive on billable EKS/MSK:

1. **commons-pool2 version (image bug).** Spark 4.0.4's Kafka connector calls
   `PoolConfig.setMinEvictableIdleDuration(Duration)`, renamed in commons-pool2
   2.12.0; I'd pinned 2.11.1 → `NoSuchMethodError` on the first Kafka read.
   Fixed: bumped 2.11.1 → **2.12.0** in all four `docker/spark-*/Dockerfile`.
2. **`event_ts` dedup (logic bug).** The Spark silver dedup ordered by
   `source_updated_at`, which is **NULL on a Debezium delete** (`after` is null),
   so an in-batch insert masked the delete and the row survived. Fixed: order the
   dedup window + MERGE guard by **`event_ts`** (source `ts_ms`, present + monotonic
   on deletes) across all four Spark silvers + the shared helper.

After both fixes: silver = 9 rows, deleted customer absent, per-country counts
match the Postgres oracle exactly (DE=2 FR=2 GB=2 SG=2 US=1). Spark pattern
validated end-to-end. Flink stacks not yet locally validated (different code
paths — LAST_VALUE / rowkind); can validate on AWS or a follow-up.

Also: `apache/iceberg-rest-fixture:1.10.1` lacks the Postgres JDBC driver (JDBC
catalog crashes) — reverted local REST catalog to `tabulario/iceberg-rest:1.6.0`.
Affects local iceberg stacks only; AWS uses Glue.

**Flink path (validated 2026-08-02, `make integration-test STACK=flink-iceberg`):** two more
bugs found + fixed:
3. **flink java17 (image bug).** `iceberg-flink-runtime-1.20-1.11.0` is compiled
   for Java 17 (class v61); the flink base image was java11 → `UnsupportedClassVersionError`.
   Fixed: both `docker/flink*/Dockerfile` → `flink:1.20.5-scala_2.12-java17`.
   NB: the 3 flink services build as separate compose images — rebuild them together.
4. **Flink 1.20 submitter config (local-only).** Flink 1.20 moved to a nested
   `config.yaml`, so `submit.sh`'s flat-key `sed` no-op'd, leaving the sql-client
   pointed at localhost → `Connection refused` on job submit. Fixed: submit.sh now
   writes a minimal flat config pointing at the JobManager. **Local-only** — on AWS
   the Flink K8s Operator submits via `FlinkDeployment` CRs, not this sql-client.

flink-iceberg result: non-direct silver = 10 rows (customer 10 = NULL tombstone,
SOFT delete); direct silver = 9 rows (customer 10 gone, HARD delete) — the
soft-vs-hard headline, validated.

**flink-paimon path (validated 2026-08-03, `make integration-test STACK=flink-paimon`):** two
more bugs found + fixed — both would have broken flink-paimon on AWS identically:
5. **create_tables.sql envsubst comment corruption.** The template's doc-comment
   listed the `${PAIMON_*}` placeholders literally; envsubst expanded them *inside
   the comment*, and because they're multi-line values they spilled raw SQL into
   the file → `CREATE CATALOG` failed (CalciteException, then a mangled split).
   Fixed: name the placeholders without `${}` in comments so envsubst skips them.
6. **Missing Hadoop bundle (image bug).** Paimon's catalog references
   `org.apache.hadoop.conf.Configuration` even with the paimon-s3 filesystem →
   `CREATE CATALOG` threw ClassNotFoundException. Fixed: added
   `flink-shaded-hadoop-2-uber-2.8.3-10.0` to `docker/flink-paimon/Dockerfile`
   (the jar flink-iceberg already carries). Needed on AWS too.

**flink-paimon result — the Objective 2 positive:** silver = 9 rows (customer 10
HARD-deleted; current view per customer_id) and **gold STREAM-READ silver**,
producing correct per-country counts (DE=2 FR=2 GB=2 SG=2 US=1) via
`changelog-producer=lookup` + Flink retraction. This is the exact chain Iceberg
cannot do (Objective 1): a single PK table that is current-view **and**
stream-readable into gold. Paimon confirmed as the format meeting all three
requirements — stream-write + current-value-per-ID + stream-read.

**spark-delta path (validated 2026-08-03, `make integration-test STACK=delta`):** two
more bugs found + fixed — both AWS-breakers:
7. **Missing delta-spark Python module.** The image shipped Delta's JARs but not the
   `delta-spark` pip package, so the DDL's `DeltaTable` builder API threw
   ModuleNotFoundError. Fixed: `pip install delta-spark==${DELTA_VERSION}`.
8. **Delta/Spark version mismatch.** `DELTA_VERSION=4.3.1` targets **Spark 4.1.0** (per
   its POM's `spark-sql_2.13` dep) and throws `NoSuchMethodError: ParserInterface.$init$`
   on our Spark 4.0.4. Corrected to **4.0.0** — the Delta release built for Spark 4.0.x
   (delta 4.1/4.2/4.3 all target Spark 4.1.0). A latent misconfig from the Spark-4 migration.

**spark-delta result:** full bronze→silver→gold PASS — silver = 9 rows (current view,
customer 10 deleted), gold = DE=2 FR=2 GB=2 SG=2 US=1. Notably gold's `readStream`
over the MERGE-updated silver did NOT hit "Detected a data update" — Delta's streaming
read tolerated the update/delete commits (gold re-reads full silver in foreachBatch).

## Status log

| Date | Update |
|------|--------|
| 2026-07-25 | Doc created. Local stack findings complete (`ARTICLE_NOTES.md`); AWS phase not yet started. |
| 2026-07-25 | Requirement refined: silver must be stream-readable, gold batch-only. Objective 3 reframed around Iceberg-view freshness. Maintenance + multi-writer notes added; Delta S3 conditional-write question resolved (not in delta-spark; DynamoDB LogStore or single-app writes). Fluss added as Objective 4. |
| 2026-07-26 | Infra model decided: ephemeral EKS per session (Flink Operator + Spark Operator, no EMR), MSK Serverless per session, in-cluster Postgres + Debezium, auto-teardown via EventBridge→CodeBuild, ~$2–3/session (~$40–60/mo). Phase plan added: Phase 1 = item_attributes only with manual inserts; Flink/Paimon optional later; Flink/Hudi and Flink/Delta excluded. |
| 2026-07-29 | Decided Spark 4.0.x + Apache Spark K8s Operator 1.0 (verified all four formats ship Spark 4.0/Scala 2.13 builds). Implemented: migrated all four `docker/spark-*/Dockerfile` to Spark 4.0.4 (Scala 2.13, hadoop-aws 3.4.1 + AWS SDK v2 2.24.6, format JARs bumped); added flink-paimon pipeline (jobs/flink-paimon/{create_tables,bronze,silver,gold}.sql + submit.sh, docker/flink-paimon/Dockerfile, compose services, Makefile targets); added env/local.env + env/aws.example.env (envsubst-based local↔AWS substitution). All 5 Spark/flink-paimon images build clean. |
| 2026-07-29 | Fixed folder-rename fallout so the local stack runs: compose mounts (`./jobs/spark`→`spark-iceberg` ×6, `./jobs/flink`→`flink-iceberg`); crossed Flink gold table refs (main gold now reads `item_attributes_flink`, direct gold reads `item_attributes_flink_direct`); flink-iceberg submit.sh job list (dropped `_v2`, added `direct/`). DDL now carries the `_direct` tables, `_v2` removed. |
| 2026-07-30 | Reskinned the whole demo from item_attributes → **customers** (fintech, "understandable to anyone"): `customers(customer_id, name, country, segment)`, gold = **active customers per country**, GDPR erasure as the natural hard-delete. Removed all item_attributes / item_inventory / item_sales / item_category scripts across every stack + dead DDL. flink-iceberg keeps BOTH silver approaches (non-direct through-bronze soft-delete + direct hard-delete) with the trade-off documented in the SQL. Source = Postgres `customers` + `gen_customers.py` (insert/update/GDPR-delete) + `pg-customers.json`. compose + Makefile rewired (`docker compose config` OK; no image rebuilds needed — jobs are volume-mounted). **Deferred: `ui/*.py`** still targets the old item schema (its kafka_cache is built around inventory reconciliation) — needs its own reskin; removed from `make all` so bring-up isn't broken by it. |
| 2026-07-29 | Bumped Flink 1.18 → 1.20.5 (both `docker/flink` and `docker/flink-paimon`): iceberg-flink-runtime → 1.11.0 (now aligned with Spark's Iceberg 1.11.0 — resolves the cross-engine version skew), flink-sql-connector-kafka → 3.4.0-1.20, flink-s3-fs-hadoop → 1.20.5, paimon-flink → 1.20 build (1.4.2). REST catalog server still tabulario/iceberg-rest:1.5.0 (local only; AWS uses Glue). Format v2 remains the cross-engine floor — still to pin explicitly in DDL. |
| 2026-08-06 | **Delta gold rewritten to CDF incremental aggregation — VALIDATED at volume.** Gold now reads silver via `readChangeFeed` (only the changes), maps `_change_type`→+1/-1, nets per country, and idempotently (`txnAppId`/`txnVersion=batchId`) MERGEs deltas into the tiny gold table — **never scans silver**; `maxFilesPerTrigger` bounds each batch incl. the one-time catch-up. Enabled `delta.enableChangeDataFeed=true` on silver at creation. Volume test (1000 base + 50 evt/s, 240s): silver emitted real update/delete commits (upd=2248 del=567 across v2–v5), gold **survived** (no crash) and matched silver's current view **exactly** (DE=84 ES=96 FR=84 GB=74 IE=87 IN=109 SG=88 US=103), processing per-batch deltas not full re-reads. Pattern + rationale documented in `STREAMING_DESIGN_PRINCIPLES.md` (§1 avoid full table scans = user's top priority). Next: same incremental shape for hudi + paimon-spark gold (flink-paimon already native retraction). |
| 2026-08-04 | **Delta gold streaming-read CONFIRMED broken at volume.** `scripts/volume_test.sh` (1000-row base + generator @50 evt/s for 240s): silver emitted real MERGE update/delete commits (v2 upd=773 del=192, then v3/v4; totals upd=1883 del=532), and gold's `readStream` on silver **crashed at silver v2** — `DELTA_SOURCE_TABLE_IGNORE_CHANGES: Detected a data update`. Gold then crash-loops and is frozen at the base-1000 state (sum≈1000) while silver's true current view is sum≈723. The low-volume integration test masked it (13-event backfill = a single insert commit). This is the delta side of the streaming-gold objective. Fix options: CDF (`readChangeFeed`) / `skipChangeCommits=true` / consume-the-changelog gold — TBD. hudi + paimon gold still to volume-test. |
| 2026-08-04 | **Objective refined: gold tables must be STREAMING reads of silver for delta/hudi/paimon** — the streaming-native formats, so gold streaming-read of silver is the whole point (vs the Iceberg track where gold is batch). Began volume/soak testing (`scripts/volume_test.sh` + `scripts/integration/volume_check_delta.py`) to validate gold under real update/delete load, not just the low-volume backfill. **Found the generator was broken:** `generator-customers` built off the deprecated streamlit image whose `requirements.txt` never listed `psycopg2` → `ModuleNotFoundError`, zero events emitted → the first delta volume run was a false pass (silver got only the 1000-row base insert; `upd=0 del=0`). Fixed: dedicated lean generator image (`docker/generator/Dockerfile`, python + psycopg2-binary), decoupled from streamlit. Re-running the volume test next. |
| 2026-08-03 | **spark-hudi validated locally (PASS)** — `make integration-test STACK=hudi`: silver 9 rows (current view), gold DE=2 FR=2 GB=2 SG=2 US=1 (gold stream-read silver via Hudi incremental query). No image/config bugs — worked first run. **Caveat (to volume-test):** low-volume backfill only; higher-volume behaviour of the streaming read not yet exercised. |
| 2026-08-03 | **spark-delta validated locally (PASS)** — `make integration-test STACK=delta`: silver 9 rows (current view), gold DE=2 FR=2 GB=2 SG=2 US=1 (gold stream-read silver via foreachBatch — no "data update" error). Fixed 2 bugs: missing `delta-spark` pip module; `DELTA_VERSION` 4.3.1→4.0.0 (4.3.1 targets Spark 4.1.0, incompatible with Spark 4.0.4). Also renamed the whole test harness `smoke`→`integration_test` (script, `scripts/integration/` dir, `make integration-test` target) and added `delta`+`hudi` cases with `integration_test_{delta,hudi}.py`. |
| 2026-08-03 | **flink-paimon validated locally (PASS)** — `make integration-test STACK=flink-paimon` green: silver = 9 rows (hard delete, current view per id), **gold stream-read silver** → DE=2 FR=2 GB=2 SG=2 US=1. Fixed 2 bugs en route (envsubst comment corruption in create_tables.sql; missing flink-shaded-hadoop in the image — both would have hit AWS). Confirms the Objective 2 front-runner: Paimon = a PK current-view table that's *also* stream-readable into gold (the chain Iceberg can't do). Added `flink-paimon` case to `scripts/integration_test.sh` + `scripts/integration/integration_test_gold_paimon.py`. |
| 2026-08-03 | **AWS deployment scope decided.** Deploy 5 stacks: (1) **flink-iceberg — bronze + silver ONLY** (no gold: purpose is to demonstrate silver `overwrite` snapshots on AWS + explain in the writeup — this is Objective 1's evidence); (2) **flink-paimon** (full bronze→silver→gold); (3) **spark-delta** (full); (4) **spark-hudi** (full); (5) **spark-paimon** (full). **Excluded: spark-iceberg** — a Spark MERGE→Iceberg silver has the same overwrite-snapshot shape as flink-iceberg, so it adds nothing to deploy. flink-paimon to be validated locally first. |
| 2026-08-02 | **Objective 1 RESOLVED (negative):** Flink cannot stream-read an Iceberg upsert/current-view silver into gold. Upsert writes equality-deletes for every row (inserts too) → `overwrite` snapshots; Flink's streaming source emits no changelog from those → gold empty after 300s with silver populated. Confirmed via `make snapshots` (append snapshots on silver are empty heartbeats; all rows land in `overwrite` commits). Decision for the Iceberg track: keep upsert current-view silver, make **gold batch**. Focus now shifts to **Objective 2 / the streaming-native formats** (Paimon front-runner: PK current-view table that is also stream-readable; Fluss next) — the goal being stream-write + current-value-per-ID + stream-read in one table. Added `make snapshots` + `scripts/integration/show_snapshots.py`. |
