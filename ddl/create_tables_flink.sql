-- Creates all Flink Iceberg tables via Flink SQL client.
-- Executed by flink-submitter before job submission.
-- Safe to re-run — all statements use CREATE ... IF NOT EXISTS.
--
-- Why not Spark SQL (create_tables.sql)?
-- Spark SQL does not support PRIMARY KEY syntax in CREATE TABLE DDL.
-- Flink's Iceberg connector requires PRIMARY KEY to be declared on the
-- table (not just identifier-field-ids) to enable changelog/upsert reads,
-- which are needed for streaming aggregations with retraction semantics
-- (e.g. GROUP BY with updates). All _flink tables are therefore created
-- here so the primary key can be declared correctly.

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

-- ─── Bronze tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.bronze.item_sales_flink (
  op                STRING,
  item_id           BIGINT,
  quantity          INT,
  total_price       DOUBLE,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  ingest_ts         TIMESTAMP(6),
  kafka_offset      BIGINT,
  kafka_partition   INT
);

CREATE TABLE IF NOT EXISTS rest.bronze.item_attributes_flink (
  op                STRING,
  item_id           BIGINT,
  name              STRING,
  price             DOUBLE,
  category          STRING,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  ingest_ts         TIMESTAMP(6),
  kafka_offset      BIGINT,
  kafka_partition   INT
);

-- ─── Silver tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.silver.item_sales_flink (
  item_id           BIGINT NOT NULL,
  quantity          INT,
  total_price       DOUBLE,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  event_date        DATE,
  ingest_ts         TIMESTAMP(6),
  commit_ts         TIMESTAMP(6),
  PRIMARY KEY (item_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS rest.silver.item_attributes_flink (
  item_id           BIGINT NOT NULL,
  name              STRING,
  price             DOUBLE,
  category          STRING,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  event_date        DATE,
  ingest_ts         TIMESTAMP(6),
  commit_ts         TIMESTAMP(6),
  PRIMARY KEY (item_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS rest.silver.item_attributes_flink_v2 (
  item_id           BIGINT NOT NULL,
  name              STRING,
  price             DOUBLE,
  category          STRING,
  source_updated_at TIMESTAMP(6),
  event_ts          TIMESTAMP(6),
  event_date        DATE,
  ingest_ts         TIMESTAMP(6),
  commit_ts         TIMESTAMP(6),
  PRIMARY KEY (item_id) NOT ENFORCED
);

-- ─── Gold tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.gold.item_category_count_flink (
  category   STRING NOT NULL,
  item_count BIGINT NOT NULL,
  PRIMARY KEY (category) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS rest.gold.item_category_count_flink_v2 (
  category   STRING NOT NULL,
  item_count BIGINT NOT NULL,
  PRIMARY KEY (category) NOT ENFORCED
);
