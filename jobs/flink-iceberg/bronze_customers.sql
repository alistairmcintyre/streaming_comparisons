-- Flink SQL: Kafka (Debezium envelope JSON) → bronze.customers_flink

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/bronze_customers/';
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

CREATE TEMPORARY TABLE kafka_customers_src (
    op       STRING,
    `before` ROW<
        customer_id BIGINT,
        name        STRING,
        country     STRING,
        segment     STRING,
        updated_at  STRING
    >,
    `after`  ROW<
        customer_id BIGINT,
        name        STRING,
        country     STRING,
        segment     STRING,
        updated_at  STRING
    >,
    `source` ROW<
        ts_ms   BIGINT,
        db      STRING,
        `table` STRING
    >,
    ts_ms    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'app.public.customers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-bronze-customers',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'false'
);

INSERT INTO rest.bronze.customers_flink
SELECT
    op,
    COALESCE(`after`.customer_id, `before`.customer_id)                  AS customer_id,
    `after`.name                                                         AS name,
    `after`.country                                                      AS country,
    `after`.segment                                                      AS segment,
    TO_TIMESTAMP(COALESCE(`after`.updated_at, `before`.updated_at), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                                 AS event_ts,
    CURRENT_TIMESTAMP                                                    AS ingest_ts,
    CAST(NULL AS BIGINT)                                                 AS kafka_offset,
    CAST(NULL AS INT)                                                    AS kafka_partition
FROM kafka_customers_src
WHERE op IS NOT NULL;
