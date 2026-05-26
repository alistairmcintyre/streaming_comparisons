-- Iceberg REST catalog JDBC backend database
-- The tabulario/iceberg-rest image auto-creates its schema tables on first start.
-- This file just ensures the DB is owned by the catalog user.
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg;
