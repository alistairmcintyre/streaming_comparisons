-- Flink SQL: Kafka (debezium-json) → silver.item_attributes_flink

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60 s';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 's3://warehouse/_flink_chk/silver_item_attributes/';
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

CREATE TEMPORARY TABLE cdc_item_attributes (
    item_id   BIGINT,
    name      STRING,
    price     DOUBLE,
    category  STRING,
    updated_at STRING,
    PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'app.public.item_attributes',
    'properties.bootstrap.servers'      = 'kafka:9092',
    'properties.group.id'               = 'flink-silver-attr',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'debezium-json',
    'debezium-json.schema-include'      = 'false',
    'debezium-json.ignore-parse-errors' = 'true'
);

INSERT INTO rest.silver.item_attributes_flink /*+ OPTIONS('upsert-enabled'='true') */
SELECT
    item_id,
    name,
    price,
    category,
    TO_TIMESTAMP(updated_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''') AS source_updated_at,
    CURRENT_TIMESTAMP                        AS event_ts,
    CAST(CURRENT_TIMESTAMP AS DATE)          AS event_date,
    CURRENT_TIMESTAMP                        AS ingest_ts,
    CURRENT_TIMESTAMP                        AS commit_ts
FROM cdc_item_attributes;
