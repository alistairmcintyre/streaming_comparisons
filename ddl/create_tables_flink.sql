-- Creates all Flink Iceberg tables via Flink SQL client.
-- Executed by flink-submitter before job submission.
-- Safe to re-run — all statements use CREATE ... IF NOT EXISTS.
--
-- Why not Spark SQL (create_tables.sql)?
-- Spark SQL does not support PRIMARY KEY syntax in CREATE TABLE DDL.
-- Flink's Iceberg connector requires PRIMARY KEY to be declared on the
-- table to enable changelog/upsert reads (needed for streaming aggregations
-- with retraction semantics, e.g. GROUP BY with updates).
--
-- Two silver tables are created for customers, to demonstrate both approaches:
--   customers_flink        — written by the NON-direct silver job (reads the
--                            bronze Iceberg table, LAST_VALUE aggregation,
--                            soft deletes). Shows streaming-from-bronze works.
--   customers_flink_direct — written by the direct silver job (reads Kafka
--                            debezium-json, hard deletes via the upsert sink).

CREATE CATALOG rest WITH (
    'type'                = 'iceberg',
    'catalog-type'        = 'rest',
    'uri'                 = 'http://iceberg-rest:8181',
    'warehouse'           = 's3://warehouse/',
    'io-impl'             = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'         = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id'    = 'minioadmin',
    's3.secret-access-key' = 'minioadmin'
);

-- ─── Bronze (append-only) ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rest.bronze.customers_flink (
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
);

-- ─── Silver (through-bronze, soft delete) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS rest.silver.customers_flink (
  customer_id       BIGINT NOT NULL,
  name              STRING,
  country           STRING,
  segment           STRING,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  event_date        DATE,
  ingest_ts         TIMESTAMP(6),
  commit_ts         TIMESTAMP(6),
  PRIMARY KEY (customer_id) NOT ENFORCED
);
ALTER TABLE rest.silver.customers_flink SET (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);

-- ─── Silver (direct-from-Kafka, hard delete) ───────────────────────────────
CREATE TABLE IF NOT EXISTS rest.silver.customers_flink_direct (
  customer_id       BIGINT NOT NULL,
  name              STRING,
  country           STRING,
  segment           STRING,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  event_date        DATE,
  ingest_ts         TIMESTAMP(6),
  commit_ts         TIMESTAMP(6),
  PRIMARY KEY (customer_id) NOT ENFORCED
);
ALTER TABLE rest.silver.customers_flink_direct SET (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);

-- ─── Gold (active customers per country) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS rest.gold.customers_per_country_flink (
  country        STRING NOT NULL,
  customer_count BIGINT NOT NULL,
  PRIMARY KEY (country) NOT ENFORCED
);
ALTER TABLE rest.gold.customers_per_country_flink SET (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);

CREATE TABLE IF NOT EXISTS rest.gold.customers_per_country_flink_direct (
  country        STRING NOT NULL,
  customer_count BIGINT NOT NULL,
  PRIMARY KEY (country) NOT ENFORCED
);
ALTER TABLE rest.gold.customers_per_country_flink_direct SET (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);
