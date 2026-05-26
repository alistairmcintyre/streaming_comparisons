-- Flink SQL: Kafka → bronze.item_inventory_flink
-- Reads the full Debezium envelope (JSON, schemas.enable=false) and appends to bronze.
-- checkpoint.interval drives commit frequency to Iceberg.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/bronze_item_inventory/';
SET 'parallelism.default' = '1';

-- ── Iceberg catalog ──────────────────────────────────────────────────────────
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

-- ── Kafka source: full Debezium envelope ─────────────────────────────────────
-- Using 'json' format (not debezium-json) to preserve op/before/after envelope.
-- Bronze should store the raw envelope including op code, offsets, etc.
CREATE TEMPORARY TABLE kafka_item_inventory_src (
    op       STRING,
    `before` ROW<
        item_id     BIGINT,
        qty_on_hand INT,
        location    STRING,
        updated_at  BIGINT
    >,
    `after`  ROW<
        item_id     BIGINT,
        qty_on_hand INT,
        location    STRING,
        updated_at  BIGINT
    >,
    `source` ROW<
        ts_ms   BIGINT,
        db      STRING,
        `table` STRING
    >,
    ts_ms    BIGINT
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'app.public.item_inventory',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-bronze-inv',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ── Insert into Iceberg bronze ────────────────────────────────────────────────
INSERT INTO rest.bronze.item_inventory_flink
SELECT
    op,
    COALESCE(`after`.item_id, `before`.item_id)                         AS item_id,
    `after`.qty_on_hand                                                  AS qty_on_hand,
    `after`.location                                                     AS location,
    -- updated_at is epoch microseconds (connect time.precision.mode=connect)
    TO_TIMESTAMP_LTZ(COALESCE(`after`.updated_at, 0) / 1000, 3)        AS source_updated_at,
    -- source.ts_ms is epoch milliseconds
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS event_ts,
    CURRENT_TIMESTAMP                                                    AS ingest_ts,
    CAST(NULL AS BIGINT)                                                 AS kafka_offset,
    CAST(NULL AS INT)                                                    AS kafka_partition
FROM kafka_item_inventory_src
WHERE op IS NOT NULL;
