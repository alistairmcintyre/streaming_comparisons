.PHONY: up down build wait create-tables register-connectors seed sql \
        start-generators start-spark start-flink start-compactor start-attr-only \
        start-delta start-hudi start-paimon \
        ui logs status all clean logs-spark logs-delta logs-hudi logs-paimon

# ─── Bring up infrastructure ────────────────────────────────────────────────

build:
	docker compose build

up:
	docker compose up -d \
		postgres-app postgres-catalog \
		kafka minio mc-init \
		iceberg-rest kafka-connect

# Wait for all services to pass healthchecks before proceeding
wait:
	@bash scripts/wait_healthy.sh

# ─── One-time setup ──────────────────────────────────────────────────────────

create-tables:
	docker compose run --rm ddl-init

register-connectors:
	@bash scripts/register_connectors.sh

# Optional: insert 1000 seed rows into Postgres (triggers Debezium CDC events into Kafka)
seed:
	docker compose exec postgres-app psql -U app -d appdb -c "\
	  INSERT INTO item_sales (item_id, quantity, total_price) \
	  SELECT i, (random()*200)::INT, round((random()*9999+1)::NUMERIC,2) \
	  FROM generate_series(1,1000) AS s(i) ON CONFLICT DO NOTHING; \
	  INSERT INTO item_attributes (item_id, name, price, category) \
	  SELECT i, 'Item-'||i, round((random()*999+1)::NUMERIC,2), \
	  (ARRAY['electronics','clothing','food','tools','toys'])[floor(random()*5+1)] \
	  FROM generate_series(1,1000) AS s(i) ON CONFLICT DO NOTHING;"
	@echo "1000 seed rows inserted into item_sales and item_attributes."

# ─── Start pipelines ─────────────────────────────────────────────────────────

start-generators:
	docker compose up -d generator-sales generator-attributes

start-spark:
	docker compose up -d \
		spark-bronze-sales spark-bronze-attr \
		spark-silver-sales spark-silver-attr \
		spark-gold-item-category-count

start-flink:
	docker compose up -d flink-jobmanager flink-taskmanager
	@echo "Waiting 10s for Flink JM to start..."
	@sleep 10
	docker compose up -d flink-submitter

start-compactor:
	docker compose up -d compactor

start-attr-only:
	docker compose up -d \
		generator-attributes \
		spark-bronze-attr \
		spark-silver-attr \
		spark-gold-item-category-count

start-delta:
	docker compose up -d ddl-init-delta
	@echo "Waiting 15s for Delta DDL init to complete..."
	@sleep 15
	docker compose up -d \
		delta-bronze-attr \
		delta-silver-attr \
		delta-gold-item-category-count

start-hudi:
	docker compose up -d \
		hudi-bronze-attr \
		hudi-silver-attr \
		hudi-gold-item-category-count

start-paimon:
	docker compose up -d \
		paimon-bronze-attr \
		paimon-silver-attr \
		paimon-gold-item-category-count

# Interactive Spark SQL shell with Iceberg catalog pre-configured
sql:
	docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-sql spark-bronze-attr \
	  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
	  --conf "spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog" \
	  --conf "spark.sql.catalog.rest.type=rest" \
	  --conf "spark.sql.catalog.rest.uri=http://iceberg-rest:8181" \
	  --conf "spark.sql.catalog.rest.warehouse=s3://warehouse/" \
	  --conf "spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
	  --conf "spark.sql.catalog.rest.s3.endpoint=http://minio:9000" \
	  --conf "spark.sql.catalog.rest.s3.path-style-access=true" \
	  --conf "spark.sql.catalog.rest.s3.access-key-id=minioadmin" \
	  --conf "spark.sql.catalog.rest.s3.secret-access-key=minioadmin" \
	  --conf "spark.sql.defaultCatalog=rest" \
	  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
	  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
	  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
	  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
	  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
	  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
	  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

# ─── Open UI ─────────────────────────────────────────────────────────────────

ui:
	docker compose up -d streamlit-ui
	@echo ""
	@echo "Streamlit UI: http://localhost:8501"
	@echo "MinIO console: http://localhost:9001  (user: minioadmin / minioadmin)"
	@echo "Flink UI:      http://localhost:8081"
	@echo "Connect REST:  http://localhost:8083"

# ─── Full bring-up in order ──────────────────────────────────────────────────

all: up
	@echo "Waiting for infra services to be healthy..."
	@bash scripts/wait_healthy.sh
	@$(MAKE) create-tables
	@$(MAKE) register-connectors
	@echo "Sleeping 15s for Debezium initial snapshot to propagate..."
	@sleep 15
	@$(MAKE) start-generators
	@$(MAKE) start-spark
	@$(MAKE) start-flink
	@$(MAKE) start-compactor
	@$(MAKE) ui
	@echo ""
	@echo "Stack is running. Open http://localhost:8501 to see the comparison UI."

# ─── Monitoring ──────────────────────────────────────────────────────────────

status:
	@echo "=== Connector status ==="
	@curl -s http://localhost:8083/connectors?expand=status | python3 -c \
		"import json,sys; d=json.load(sys.stdin); [print(f'  {n}: {i[\"status\"][\"connector\"][\"state\"]}') for n,i in d.items()]" \
		2>/dev/null || echo "  (kafka-connect not reachable)"
	@echo ""
	@echo "=== Docker services ==="
	@docker compose ps

logs:
	docker compose logs -f --tail=50

logs-spark:
	docker compose logs -f --tail=50 spark-bronze-sales spark-bronze-attr spark-silver-sales spark-silver-attr spark-gold-item-category-count

logs-flink:
	docker compose logs -f --tail=50 flink-jobmanager flink-taskmanager flink-submitter

logs-delta:
	docker compose logs -f --tail=50 ddl-init-delta delta-bronze-attr delta-silver-attr delta-gold-item-category-count

logs-hudi:
	docker compose logs -f --tail=50 hudi-bronze-attr hudi-silver-attr hudi-gold-item-category-count

logs-paimon:
	docker compose logs -f --tail=50 paimon-bronze-attr paimon-silver-attr paimon-gold-item-category-count

logs-connect:
	docker compose logs -f --tail=50 kafka-connect

# ─── Teardown ────────────────────────────────────────────────────────────────

down:
	docker compose down

# Remove ALL data including volumes (destructive — use with care)
clean:
	docker compose down -v
	@echo "All containers and volumes removed."
