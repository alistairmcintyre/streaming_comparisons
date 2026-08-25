-- Flink SQL: silver.customers_flink → gold.customers_per_country_flink
--
-- Streams the NON-direct silver table as a changelog and maintains a running
-- count of active customers per country. Category/country changes decrement the
-- old country and increment the new via Flink's retraction mechanism.
--
-- WHERE country IS NOT NULL excludes soft-delete tombstones (deleted customers
-- carry a NULL country in the non-direct silver) — the cost of soft deletes.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/gold_customers_per_country/';
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
    customer_id       BIGINT,
    name              STRING,
    country           STRING,
    segment           STRING,
    source_updated_at TIMESTAMP(6),
    event_ts          TIMESTAMP(6),
    event_date        DATE,
    ingest_ts         TIMESTAMP(6),
    commit_ts         TIMESTAMP(6),
    PRIMARY KEY (customer_id) NOT ENFORCED
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
    'catalog-table'        = 'customers_flink',
    'streaming'            = 'true',
    'monitor-interval'     = '10s',
    'starting-strategy'    = 'TABLE_SCAN_THEN_INCREMENTAL'
);

INSERT INTO rest.gold.customers_per_country_flink /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    country,
    COUNT(customer_id) AS customer_count
FROM silver_src
WHERE country IS NOT NULL
GROUP BY country;
