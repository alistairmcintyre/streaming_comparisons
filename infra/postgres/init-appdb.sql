-- Application database schema
-- Requires: postgres started with wal_level=logical (set in docker-compose command args)

CREATE TABLE IF NOT EXISTS item_sales (
    item_id     BIGINT PRIMARY KEY,
    quantity    INT          NOT NULL DEFAULT 0,
    total_price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS item_attributes (
    item_id   BIGINT          PRIMARY KEY,
    name      TEXT            NOT NULL,
    price     NUMERIC(10, 2)  NOT NULL DEFAULT 0.00,
    category  TEXT            NOT NULL DEFAULT 'general',
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Debezium requires REPLICA IDENTITY FULL to capture before-images on UPDATE/DELETE.
-- Without this, 'before' is null in all change events — Flink upsert and delete
-- handling in bronze jobs both break silently.
ALTER TABLE item_sales       REPLICA IDENTITY FULL;
ALTER TABLE item_attributes  REPLICA IDENTITY FULL;

-- Pre-create the publication covering both tables so Debezium connectors
-- (which use publication.autocreate.mode=filtered) don't race to create it
-- and end up with only one table registered.
CREATE PUBLICATION dbz_publication FOR TABLE item_sales, item_attributes;
