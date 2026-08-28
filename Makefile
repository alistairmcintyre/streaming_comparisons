.PHONY: up down build wait create-tables register-connectors sql \
        start-generators start-spark start-compactor \
        start-delta start-flink-paimon \
        ui logs status all clean logs-spark logs-delta logs-flink-paimon test test-all

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
	@echo "Tables are now created IN the pipelines (idempotent ensure_all at job startup)."
	@echo "No separate DDL step — start the pipelines directly."

register-connectors:
	@bash scripts/register_connectors.sh

# ─── Start pipelines ─────────────────────────────────────────────────────────

start-generators:
	docker compose up -d generator-accounts generator-trades

start-spark:
	docker compose up -d \
		spark-bronze-trades spark-silver-trades spark-silver-accounts spark-gold-openpositions

start-compactor:
	docker compose up -d compactor

start-delta:
	docker compose up -d \
		delta-bronze-trades \
		delta-silver-trades \
		delta-silver-accounts \
		delta-gold-openpositions

start-flink-paimon:
	docker compose up -d flink-paimon-jobmanager flink-paimon-taskmanager
	@echo "Waiting 10s for Flink Paimon JM to start..."
	@sleep 10
	docker compose up -d flink-paimon-submitter

# Interactive Spark SQL shell with Iceberg catalog pre-configured
sql:
	docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-sql spark-bronze-trades \
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
	@$(MAKE) start-compactor
	@echo ""
	@echo "Stack is running."

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
	docker compose logs -f --tail=50 spark-bronze-trades spark-silver-trades spark-silver-accounts spark-gold-openpositions

logs-delta:
	docker compose logs -f --tail=50 delta-bronze-trades delta-silver-trades delta-gold-openpositions

logs-flink-paimon:
	docker compose logs -f --tail=50 flink-paimon-jobmanager flink-paimon-taskmanager flink-paimon-submitter

logs-connect:
	docker compose logs -f --tail=50 kafka-connect

# ─── Teardown ────────────────────────────────────────────────────────────────

down:
	docker compose down

# Remove ALL data including volumes (destructive — use with care)
clean:
	docker compose down -v
	@echo "All containers and volumes removed."


# ── pre-deploy checks ───────────────────────────────────────────────────────
# Run after every change. `test` is seconds and needs nothing; `test-all` adds the
# Flink SQL compile for BOTH engines (Docker, ~6 min: paimon needs a Flink, fluss also
# needs zookeeper + coordinator + tablet server). Both tiers also verify each CHECKER can still fail
# against a known-bad fixture — several checks written on 2026-08-27 were themselves
# broken in ways that made them silently pass, which is how ~$42 of clusters went on
# bugs that were free to catch.
test:
	./tests/run-checks.sh

test-all:
	./tests/run-checks.sh --all
