-- Creates Spark Iceberg namespaces and tables via Spark SQL.
-- Executed by the ddl-init container using spark-submit.
-- Safe to re-run — all statements use CREATE ... IF NOT EXISTS.
--
-- Flink tables are created separately in create_tables_flink.sql by
-- flink-submitter, because Spark SQL does not support PRIMARY KEY syntax
-- which is required by the Flink Iceberg connector for changelog/upsert reads.

CREATE NAMESPACE IF NOT EXISTS rest.bronze;
CREATE NAMESPACE IF NOT EXISTS rest.silver;
CREATE NAMESPACE IF NOT EXISTS rest.gold;

-- ─── Bronze tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.bronze.item_sales_spark (
  op                STRING,
  item_id           BIGINT,
  quantity          INT,
  total_price       DOUBLE,
  source_updated_at TIMESTAMP,
  event_ts          TIMESTAMP,
  ingest_ts         TIMESTAMP,
  kafka_offset      BIGINT,
  kafka_partition   INT
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes'    = '134217728',
  'commit.retry.num-retries'        = '10',
  'commit.retry.min-wait-ms'        = '200',
  'write.distribution-mode'         = 'none',
  'write.spark.fanout.enabled'      = 'true'
);

CREATE TABLE IF NOT EXISTS rest.bronze.item_attributes_spark (
  op                STRING,
  item_id           BIGINT,
  name              STRING,
  price             DOUBLE,
  category          STRING,
  source_updated_at TIMESTAMP,
  event_ts          TIMESTAMP,
  ingest_ts         TIMESTAMP,
  kafka_offset      BIGINT,
  kafka_partition   INT
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes'    = '134217728',
  'commit.retry.num-retries'        = '10',
  'commit.retry.min-wait-ms'        = '200',
  'write.distribution-mode'         = 'none',
  'write.spark.fanout.enabled'      = 'true'
);

-- ─── Silver tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.silver.item_sales_spark (
  item_id           BIGINT NOT NULL,
  quantity          INT,
  total_price       DOUBLE,
  source_updated_at TIMESTAMP,
  event_ts          TIMESTAMP,
  event_date        DATE,
  ingest_ts         TIMESTAMP,
  commit_ts         TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes'    = '134217728',
  'commit.retry.num-retries'        = '10',
  'commit.retry.min-wait-ms'        = '200',
  'write.delete.mode'               = 'merge-on-read',
  'write.update.mode'               = 'merge-on-read',
  'write.merge.mode'                = 'merge-on-read',
  'write.distribution-mode'         = 'hash',
  'write.spark.fanout.enabled'      = 'true'
);

CREATE TABLE IF NOT EXISTS rest.silver.item_attributes_spark (
  item_id           BIGINT NOT NULL,
  name              STRING,
  price             DOUBLE,
  category          STRING,
  source_updated_at TIMESTAMP,
  event_ts          TIMESTAMP,
  event_date        DATE,
  ingest_ts         TIMESTAMP,
  commit_ts         TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes'    = '134217728',
  'commit.retry.num-retries'        = '10',
  'commit.retry.min-wait-ms'        = '200',
  'write.delete.mode'               = 'merge-on-read',
  'write.update.mode'               = 'merge-on-read',
  'write.merge.mode'                = 'merge-on-read'
);

-- ─── Gold tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS rest.gold.item_category_count_spark (
  category   STRING NOT NULL,
  item_count BIGINT,
  commit_ts  TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes'    = '134217728',
  'commit.retry.num-retries'        = '10',
  'commit.retry.min-wait-ms'        = '200',
  'write.delete.mode'               = 'merge-on-read',
  'write.update.mode'               = 'merge-on-read',
  'write.merge.mode'                = 'merge-on-read',
  'write.upsert.enabled'            = 'true',
  'identifier-field-ids'            = '1'
);
