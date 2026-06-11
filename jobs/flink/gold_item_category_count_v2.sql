-- Flink SQL: silver.item_attributes_flink_v2 → gold.item_category_count_flink_v2
--
-- Stream reads the v2 silver table which is written via bronze Iceberg source
-- + LAST_VALUE SCD1 aggregation + upsert sink. Because v2 silver produces
-- append + equality-delete snapshots (not overwrite), Flink can read it as
-- a proper changelog and derive retractions for category changes.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/gold_item_category_count_v2/';
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

CREATE TEMPORARY TABLE silver_src (
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
    'connector'            = 'iceberg',
    'catalog-name'         = 'rest',
    'catalog-type'         = 'rest',
    'uri'                  = 'http://iceberg-rest:8181',
    'warehouse'            = 's3://warehouse/',
    'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'          = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id'     = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    'catalog-database'     = 'silver',
    'catalog-table'        = 'item_attributes_flink_v2',
    'streaming'            = 'true',
    'monitor-interval'     = '10s',
    'starting-strategy'   = 'TABLE_SCAN_THEN_INCREMENTAL'
);

INSERT INTO rest.gold.item_category_count_flink_v2 /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    category,
    COUNT(item_id) AS item_count
FROM silver_src
GROUP BY category;
