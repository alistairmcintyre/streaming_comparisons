# Fluss hot tier → Paimon → Iceberg view

The streaming-native stack: **Fluss (hot, sub-second) → tiers to Paimon (warm
changelog/current-view) → Paimon's Iceberg view (cold/interop)**. Two physical
stores, three read surfaces (Fluss KV, Paimon changelog, Iceberg snapshot).

```
Kafka trades ─► Fluss silver.trades (PK, hot) ─► fold in Flink ─► Fluss gold.open_positions (PK book)
Kafka accounts ─► Fluss silver.accounts (PK dimension) ─┘
                       │  datalake.enabled + fluss-flink-tiering job
                       ▼
               Paimon  s3://warehouse/fluss/paimon   ──►  Iceberg view (Athena/Trino/Spark)
```

> **Status: validated end-to-end** (Fluss 0.9.1 + Paimon 1.3.1, local). Kafka →
> bronze → Fluss `trades` → gold fold → Fluss `open_positions` → tiering → Paimon
> on MinIO. Fold is EXACT on the tiered Paimon tables: `signed_qty == net_quantity`
> (19,696,551) and `trades_rows == Σtrade_count` (386,000), 20,000 position rows.

## Images (why we build, not just pull)

`docker/fluss` (coordinator/tablet) adds **paimon-s3** to the paimon plugin dir —
the base `apache/fluss` image has paimon-bundle but not paimon-s3, so the datalake
warehouse can't init. `docker/fluss-flink` (JM/TM) adds to `/opt/flink/lib`:
`flink-sql-connector-kafka` (bronze source), `fluss-lake-paimon` (datalake-enabled
tables need the lake plugin on the client), `paimon-flink-1.20`, `paimon-s3`, and
`flink-shaded-hadoop-2-uber`. See `docker/*/Dockerfile`.

## remote.data.dir is a shared LOCAL volume, not S3

Fluss vends S3 access to clients via STS `GetSessionToken`, which MinIO doesn't
implement (client hits real AWS STS → 403). That blocks the read path (gold) while
writes (bronze) succeed. So `remote.data.dir` uses the named volume `fluss-remote`
(mounted on coordinator+tablet+JM+TM); the Paimon **datalake** tier still lives on
S3 (`datalake.paimon.*`, written by the tiering job, which uses paimon-s3 directly).

## Config is env-driven (local ⇄ AWS)

Everything switches on `DEPLOY_ENV` + scalars in `env/<env>.env` — the same
convention as the flink-paimon stack. `jobs/flink-fluss/submit.sh` renders the SQL
templates (`create_tables/silver_trades/silver_accounts/gold_open_positions.sql`) with `envsubst`
and starts the tiering job; `docker-compose.fluss.yml` reads `${VAR:-default}` for
the server/Flink config. The knobs (see `env/local.env` / `env/aws.example.env`):
`FLUSS_BOOTSTRAP`, `DATALAKE_FORMAT`, `FLUSS_PAIMON_WAREHOUSE`, `FLUSS_REMOTE_DATA_DIR`,
`PAIMON_ICEBERG_STORAGE`, `S3_*`, `KAFKA_BOOTSTRAP`, `AWS_REGION`.

## 1. Build the source-built Fluss artifacts (once, or when main moves)

```bash
FLUSS_SRC=~/git/apache/fluss docker/fluss/build-fluss.sh   # JDK11 maven build in a container
```

## 2. Bring it up — one command does create + submit + tiering

```bash
# base infra (kafka, minio, postgres, connect) + generators first:
make up && make register-connectors && make start-generators

docker compose -f docker-compose.yml -f docker-compose.fluss.yml up -d \
  fluss-zookeeper fluss-coordinator fluss-tablet-server \
  fluss-flink-jobmanager fluss-flink-taskmanager fluss-flink-submitter
```

`fluss-flink-submitter` waits for the JobManager, creates the tables (retrying past
the coordinator-warmup race), submits bronze + gold, and starts the Fluss→Paimon
tiering job. Flink UI: http://localhost:8085.

## 3. Verify — read the tiered Paimon tables directly

Batch-reading the Fluss tables (`SET 'execution.runtime-mode'='batch'`) needs a lake
snapshot to exist AND S3 config the `fluss` catalog doesn't carry, so it throws
`no file io for scheme 's3'`. Read the **Paimon** tables directly instead — a Paimon
catalog with S3 inline, exactly what a downstream Spark/Trino/Athena consumer does:

```sql
SET 'execution.runtime-mode' = 'batch';
CREATE CATALOG paimon_lake WITH (
  'type'='paimon', 'warehouse'='s3://warehouse/fluss/paimon',
  's3.endpoint'='http://minio:9000', 's3.access-key'='minioadmin',
  's3.secret-key'='minioadmin', 's3.path.style.access'='true'
);
-- fold oracle (stop the generator + let it drain for an EXACT match):
SELECT COUNT(*) AS trades_rows,
       SUM(CASE WHEN side='BUY' THEN CAST(quantity AS BIGINT) ELSE -CAST(quantity AS BIGINT) END) AS signed_qty
FROM paimon_lake.silver.trades;
SELECT COUNT(*) AS pos_rows, SUM(net_quantity) AS net_qty, SUM(trade_count) AS tcount
FROM paimon_lake.gold.open_positions;
```

Expected: `signed_qty == net_qty` and `trades_rows == tcount`. The Paimon tables live
under `s3://warehouse/fluss/paimon` — the spark-paimon image can read them the same way.

## Iceberg-compat for Athena — SOLVED (requires Fluss built from `main`)

Goal: the tiered Paimon tables expose an Iceberg view Athena can read. Set
`'paimon.metadata.iceberg.storage' = 'hadoop-catalog'` in the Fluss table WITH clause
(Fluss forwards `paimon.`-prefixed options to the tiered Paimon table) — see
`create_tables.sql`. On AWS swap to `'hive-catalog'` + the Glue client so Athena
discovers the tables (same block as `jobs/flink-paimon/submit.sh`).

**This does NOT work on the 0.9.1 release.** Fluss appends system columns to the lake
table, and `__timestamp` is `TIMESTAMP_LTZ(3)`; Paimon's Iceberg-compat rejects
millisecond precision ("only support timestamp type with precision from 4 to 9"), so no
Iceberg `metadata.json` is ever written and the table is invisible to Athena/Spark.
**FIP-27** (apache/fluss#3902, PR #3982, on `main` since 2026-08-17) gives new lake tables
a *clean* schema with no `__bucket/__offset/__timestamp`. It is not
in any release yet though (latest is 0.9.1-incubating, 2026-05-04), so we build from source:

```bash
FLUSS_SRC=~/git/apache/fluss docker/fluss/build-fluss.sh   # JDK11 maven build in a container
```

That stages the 1.0-SNAPSHOT artifacts our images consume (`docker/fluss` = server dist,
`docker/fluss-flink` = shaded connector + tiering + lake-paimon + Paimon 1.4.2). Also note:
- **Rebuild all four images** (coordinator, tablet, JM, TM) together — a stale image vs a
  fresh one gives `InvalidClassException: serialVersionUID` / `ClassNotFoundException`.
- The TM needs real memory (`taskmanager.memory.process.size: 4096m`) — the quickstart
  default ~512MB heap OOMs running bronze+gold+tiering, killing the whole cluster.
- The JM/TM need the datalake `s3.*` config too, so the Fluss source's **hybrid lake read**
  (Fluss log + Paimon snapshot) can read the tiered splits (else gold fails with
  "Failed to generate hybrid lake fluss splits").

VERIFIED locally: tiered `trades` and `open_positions` get complete Iceberg metadata
(`v*.metadata.json` + `version-hint.text`) and spark-iceberg (hadoop catalog, s3a) reads
both, which is the Athena path end to end.

## Deploying to AWS

Copy `env/aws.example.env` → `env/aws.env`, fill in the `REPLACE_ME` values, then point
the stack at it (`set -a && source env/aws.env && set +a` before compose up, and swap the
submitter's `env_file` to `env/aws.env`). `DEPLOY_ENV=aws` flips these automatically:

| Concern | local | AWS (`DEPLOY_ENV=aws`) |
|---|---|---|
| Iceberg view | `hadoop-catalog` on S3 | `hive-catalog` → **Glue** (Athena reads the tables) |
| Kafka | `kafka:9092` | **MSK** bootstrap + IAM SASL (`AWS_MSK_IAM`) |
| Object store | MinIO endpoint + keys | S3 via **IAM role** (no endpoint/keys; `AWS_REGION` only) |
| `remote.data.dir` | shared volume (MinIO can't do STS) | **EFS RWX**, not S3 — IRSA's assumed-role creds can't call STS `GetSessionToken`, which Fluss's remote-log vendor needs (see infra/aws/efs.tf). Paimon datalake stays on S3. |
| Iceberg format | v2 (Athena-compatible) | v2 — keep it (see repo `AWS_OBJECTIVES.md` Obj. 2) |

The **server** (coordinator/tablet) and **Flink cluster** are the built images
(`docker/fluss`, `docker/fluss-flink`); on AWS run them on EKS/EC2/EMR with the same
`FLUSS_PROPERTIES` / `FLINK_PROPERTIES`, S3 via the task/pod IAM role. Blank S3 keys are
dropped from `server.yaml` by the entrypoint so Fluss uses the default (IAM) credential
provider. Athena then queries `silver.trades` / `gold.open_positions` directly
from Glue. Keep Iceberg **format-version 2** (Athena requirement).


## Layer naming (why Fluss has a `silver` and no `bronze`)

Fluss's PK table already does what bronze→silver does elsewhere: it dedupes on the
key and serves the cleaned current view, with no separate landing table to re-read.
Calling it `bronze` would have implied a hop that does not exist; calling it
`silver` states what it holds. So the databases are `silver` (trades, accounts) and
`gold` (open_positions) — the same layer names, meaning the same things, as the
other four engines, which is what lets the reconciliation address them uniformly.

The saved hop is real and is Fluss's structural advantage, not something to hide in
the naming: `results.json` records `hops` per engine (Fluss 2, the rest 3) so a
latency comparison can be read against the topology that produced it.

## Timestamp semantics in gold

    opened_at       MIN(executed_at)  — event time of the fill that opened the position
    last_updated_at MAX(executed_at)  — event time of the newest fill
    commit_ts       processing time when the gold row was produced

`commit_ts - last_updated_at` is the per-row processing delay, computed identically
in all five engines and readable straight out of the table — no latency-emit chain
in the loop. `snapshot-results.sh` reports its percentiles as the headline metric.

`opened_at` is MIN over the position's whole history, not "reset each time the
position goes flat and reopens". Reset-on-flat is easy in the Spark MERGE and not
expressible as a pure Flink fold, so adopting it would make the two families hold
different values for identical input and break cross-engine reconciliation.
