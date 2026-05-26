-- Flink SQL: Kafka (debezium-json) → silver.item_inventory_flink
--
-- Uses 'debezium-json' format which maps Debezium c/u/r → INSERT/UPDATE (+I/+U)
-- and d → DELETE (-D) as Flink RowKinds. Combined with PRIMARY KEY NOT ENFORCED,
-- the Iceberg sink runs in native upsert mode.
--
-- This reads from Kafka directly (not bronze) to minimize latency.
-- The comparison point is the silver table output, not the source path.
--
-- Why debezium-json here but json in bronze:
--   Bronze wants the raw envelope (op, before, after, offsets) for auditability.
--   Silver wants the changelog stream (RowKind) for upsert semantics.
--   Different shapes; don't mix them.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/silver_item_inventory/';
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

-- Source: Kafka with debezium-json format.
-- The PRIMARY KEY NOT ENFORCED causes Flink to track state per item_id
-- and emit proper RowKind (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE).
CREATE TEMPORARY TABLE cdc_item_inventory (
    item_id     BIGINT,
    qty_on_hand INT,
    location    STRING,
    updated_at  BIGINT,   -- epoch microseconds, converted below in the SELECT
    PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'app.public.item_inventory',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-silver-inv',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'debezium-json',
    'debezium-json.schema-include'  = 'false',
    'debezium-json.ignore-parse-errors' = 'true'
);

-- Sink: Iceberg silver table in upsert mode.
-- The table was created with write.upsert.enabled=true and identifier_field_ids=[item_id].
-- The /*+ OPTIONS(...) */ hint makes the upsert mode explicit at query level.
INSERT INTO rest.silver.item_inventory_flink /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    item_id,
    qty_on_hand,
    location,
    TO_TIMESTAMP_LTZ(updated_at / 1000, 3)                              AS source_updated_at,
    CURRENT_TIMESTAMP                                                    AS event_ts,
    CAST(CURRENT_TIMESTAMP AS DATE)                                      AS event_date,
    CURRENT_TIMESTAMP                                                    AS ingest_ts,
    CURRENT_TIMESTAMP                                                    AS commit_ts
FROM cdc_item_inventory;
