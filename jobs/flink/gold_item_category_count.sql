-- Flink SQL: silver.item_attributes_flink → gold.item_category_count_flink
--
-- Stream reads the Flink silver table as a changelog (upsert mode).
-- Groups by category and maintains a running item count.
-- Because silver uses Flink upsert writes (append + equality-delete snapshots),
-- the Iceberg source emits a proper changelog that Flink can aggregate correctly.
-- Category changes (UPDATE on an item) decrement the old category count and
-- increment the new one automatically via Flink's retraction mechanism.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/gold_item_category_count/';
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

-- Read silver as a changelog stream.
-- The primary key tells Flink this is an upsert source so it can
-- derive retractions when a row is updated or deleted.
CREATE TEMPORARY TABLE silver_item_attributes (
    item_id           BIGINT,
    name              STRING,
    price             DOUBLE,
    category          STRING,
    source_updated_at TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    event_date        DATE,
    ingest_ts         TIMESTAMP(6),
    commit_ts         TIMESTAMP(6),
    PRIMARY KEY (item_id) NOT ENFORCED
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
    'catalog-database'    = 'silver',
    'catalog-table'       = 'item_attributes_flink',
    'streaming'           = 'true',
    'monitor-interval'    = '10s',
    'starting-strategy'   = 'TABLE_SCAN_THEN_INCREMENTAL'
);

INSERT INTO rest.gold.item_category_count_flink /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    category,
    COUNT(item_id) AS item_count
FROM silver_item_attributes
GROUP BY category;
