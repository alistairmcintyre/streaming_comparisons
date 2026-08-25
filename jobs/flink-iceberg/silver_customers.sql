-- Flink SQL: bronze.customers_flink → silver.customers_flink  (NON-DIRECT approach)
--
-- Streams FROM the bronze Iceberg table (the medallion path). Because bronze is
-- append-only and the op is carried as a data column, this approach cannot emit a
-- real DELETE — a LAST_VALUE GROUP BY key never leaves the aggregation. So a
-- deleted customer (op='d') becomes a SOFT delete: a tombstone row with its
-- attribute columns nulled out. Downstream aggregations must exclude these
-- (see gold: WHERE country IS NOT NULL).
--
-- This is kept deliberately to show that streaming-from-bronze IS possible and is
-- a hard requirement in some setups — at the cost of soft-delete semantics.
-- Compare with direct/silver_customers.sql, which reads the Kafka changelog
-- directly and performs real hard deletes.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/silver_customers/';
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

CREATE TEMPORARY TABLE bronze_src (
    op                STRING,
    customer_id       BIGINT,
    name              STRING,
    country           STRING,
    segment           STRING,
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
    'catalog-table'       = 'customers_flink',
    'streaming'           = 'true',
    'monitor-interval'    = '10s',
    'starting-strategy'   = 'TABLE_SCAN_THEN_INCREMENTAL'
);

-- SCD1 via LAST_VALUE per customer_id (bounded state: one row per customer).
CREATE TEMPORARY VIEW deduped AS
SELECT
    customer_id,
    LAST_VALUE(op)          AS op,
    LAST_VALUE(name)        AS name,
    LAST_VALUE(country)     AS country,
    LAST_VALUE(segment)     AS segment,
    MAX(source_updated_at)  AS source_updated_at,
    MAX(event_ts)           AS event_ts,
    MAX(ingest_ts)          AS ingest_ts
FROM bronze_src
WHERE op IS NOT NULL
GROUP BY customer_id;

INSERT INTO rest.silver.customers_flink
    /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    customer_id,
    CASE WHEN op = 'd' THEN NULL ELSE name END    AS name,
    CASE WHEN op = 'd' THEN NULL ELSE country END AS country,
    CASE WHEN op = 'd' THEN NULL ELSE segment END AS segment,
    source_updated_at,
    event_ts,
    CAST(event_ts AS DATE)                        AS event_date,
    ingest_ts,
    CURRENT_TIMESTAMP                             AS commit_ts
FROM deduped;
