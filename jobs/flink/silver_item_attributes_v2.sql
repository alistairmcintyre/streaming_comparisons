-- Flink SQL: bronze.item_attributes_flink → silver.item_attributes_flink_v2
--
-- Reads from bronze Iceberg (append-only, has op column) and emits a proper
-- Flink changelog stream by mapping the Debezium op field to Flink RowKind
-- using a CASE expression. The Iceberg upsert sink then uses the
-- EqualityDeltaWriter path, producing append + equality-delete snapshots
-- (not overwrite) — making this table stream-readable downstream.
--
-- op mapping:
--   'c' (create) / 'r' (read/snapshot) → INSERT  (+I)
--   'u' (update)                        → UPDATE_AFTER (+U, Flink handles -U internally)
--   'd' (delete)                        → DELETE (-D)
--
-- The changelog is emitted by declaring the source table with a PRIMARY KEY
-- and setting 'changelog-mode' = 'upsert', which tells Flink the stream
-- contains upsert semantics. Flink derives retractions automatically.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/silver_item_attributes_v2/';
SET 'parallelism.default' = '1';

CREATE CATALOG rest WITH (
    'type'               = 'iceberg',
    'catalog-type'       = 'rest',
    'uri'                = 'http://iceberg-rest:8181',
    'warehouse'          = 's3://warehouse/',
    'io-impl'            = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'        = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id'   = 'minioadmin',
    's3.secret-access-key' = 'minioadmin'
);

-- Read bronze directly from the catalog as a streaming source.
-- Using the catalog table reference (rest.bronze.item_attributes_flink) rather
-- than the connector approach avoids database-name resolution issues.
-- TABLE_SCAN_THEN_INCREMENTAL ensures existing rows are read before streaming new ones.
CREATE TEMPORARY TABLE bronze_src (
    op                STRING,
    item_id           BIGINT,
    name              STRING,
    price             DOUBLE,
    category          STRING,
    source_updated_at TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    ingest_ts         TIMESTAMP(6),
    kafka_offset      BIGINT,
    kafka_partition   INT
) WITH (
    'connector'           = 'iceberg',
    'catalog-name'        = 'rest',
    'catalog-type'        = 'rest',
    'uri'                 = 'http://iceberg-rest:8181',
    'warehouse'           = 's3://warehouse/',
    'io-impl'             = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'         = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id'    = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    'catalog-database'    = 'bronze',
    'catalog-table'       = 'item_attributes_flink',
    'streaming'           = 'true',
    'monitor-interval'    = '10s',
    'starting-strategy'   = 'TABLE_SCAN_THEN_INCREMENTAL'
);

-- SCD1 aggregation using LAST_VALUE per item_id.
-- Flink maintains one row of state per item_id and updates it as new events
-- arrive, emitting a retraction (-U) + new value (+U) on each change.
-- This is more efficient than ROW_NUMBER() as state is bounded to one
-- aggregated row per item_id rather than the full event history.
-- MAX(source_updated_at) guards against late arrivals — LAST_VALUE columns
-- are only meaningful when the event is the newest seen for that item_id.
CREATE TEMPORARY VIEW deduped AS
SELECT
    item_id,
    LAST_VALUE(op)                AS op,
    LAST_VALUE(name)              AS name,
    LAST_VALUE(price)             AS price,
    LAST_VALUE(category)          AS category,
    MAX(source_updated_at)        AS source_updated_at,
    MAX(event_ts)                 AS event_ts,
    MAX(ingest_ts)                AS ingest_ts
FROM bronze_src
WHERE op IS NOT NULL
GROUP BY item_id;

-- Write to silver via Iceberg upsert sink.
-- upsert-enabled=true + primary key on target activates EqualityDeltaWriter:
--   inserts/updates → append snapshot (new data file)
--   deletes (op='d') → equality-delete file (delete snapshot)
-- Both are streaming-readable downstream.
INSERT INTO rest.silver.item_attributes_flink_v2
    /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    item_id,
    CASE WHEN op = 'd' THEN NULL ELSE name END            AS name,
    CASE WHEN op = 'd' THEN NULL ELSE price END           AS price,
    CASE WHEN op = 'd' THEN NULL ELSE category END        AS category,
    source_updated_at,
    event_ts,
    CAST(event_ts AS DATE)                                AS event_date,
    ingest_ts,
    CURRENT_TIMESTAMP                                     AS commit_ts
FROM deduped;
