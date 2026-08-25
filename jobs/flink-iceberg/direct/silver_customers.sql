-- Flink SQL: Kafka (debezium-json) → silver.customers_flink_direct  (DIRECT approach)
--
-- Reads the CDC changelog DIRECTLY from Kafka using format=debezium-json, which
-- decodes op='d' into a native -D RowKind. The Iceberg upsert sink then performs
-- a REAL hard delete (equality-delete, no tombstone). This is the "straight to
-- silver" pattern — it skips the bronze table but gets hard deletes for free.
--
-- Compare with the non-direct silver_customers.sql, which streams from the bronze
-- Iceberg table but can only soft-delete.

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/silver_customers_direct/';
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

CREATE TEMPORARY TABLE cdc_customers (
    customer_id BIGINT,
    name        STRING,
    country     STRING,
    segment     STRING,
    updated_at  STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'app.public.customers',
    'properties.bootstrap.servers'      = 'kafka:9092',
    'properties.group.id'               = 'flink-silver-customers-direct',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'debezium-json',
    'debezium-json.schema-include'      = 'false',
    'debezium-json.ignore-parse-errors' = 'true'
);

INSERT INTO rest.silver.customers_flink_direct /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    customer_id,
    name,
    country,
    segment,
    TO_TIMESTAMP(updated_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    CURRENT_TIMESTAMP               AS event_ts,
    CAST(CURRENT_TIMESTAMP AS DATE) AS event_date,
    CURRENT_TIMESTAMP               AS ingest_ts,
    CURRENT_TIMESTAMP               AS commit_ts
FROM cdc_customers;
