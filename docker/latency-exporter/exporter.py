"""
Pipeline latency exporter for the "Pipeline Comparison. Live" dashboard.

Consumes the `pipeline_latency` Kafka topic, each event is a sampled per-record
timing from a pipeline: {"pipeline": "...", "executed_at_ms": <int>, "ingest_ts_ms": <int>}.
It:
  - observes delay_ms/1000 into a Prometheus Histogram processing_delay_seconds{pipeline}
    → Grafana plots p50/p95/p99 OVER TIME via histogram_quantile (not a 1h snapshot).
  - appends the raw events to S3 as Parquet (survives the 2.5h teardown) for exact
    offline percentiles / rate-of-change with Athena or DuckDB.

Env: KAFKA_BOOTSTRAP, LATENCY_TOPIC, S3_BENCHMARK_PREFIX (s3://bucket/benchmarks/<RunId>/latency),
RUN_ID. S3 creds via IRSA (default chain).
"""
import json
import os
import time
import io
import threading

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Histogram, Counter, start_http_server

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "trades-kafka-bootstrap.kafka.svc:9092")
TOPIC = os.environ.get("LATENCY_TOPIC", "pipeline_latency")
S3_PREFIX = os.environ.get("S3_BENCHMARK_PREFIX", "")  # s3://bucket/benchmarks/<RunId>/latency
RUN_ID = os.environ.get("RUN_ID", "manual")
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "60"))

# Buckets in seconds, tuned for streaming freshness (10ms .. 5m).
BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)
DELAY = Histogram("processing_delay_seconds", "event->processed delay", ["pipeline"], buckets=BUCKETS)
EVENTS = Counter("pipeline_latency_events_total", "events consumed", ["pipeline"])

_buf = []
_lock = threading.Lock()


def _flush_loop():
    s3 = boto3.client("s3")
    while True:
        time.sleep(FLUSH_SECONDS)
        with _lock:
            rows, _buf[:] = _buf[:], []
        if not rows or not S3_PREFIX:
            continue
        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        # s3://bucket/prefix -> bucket, prefix
        _, _, rest = S3_PREFIX.partition("s3://")
        bucket, _, prefix = rest.partition("/")
        key = f"{prefix.rstrip('/')}/part-{int(time.time())}.parquet"
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"flushed {len(rows)} events -> s3://{bucket}/{key}", flush=True)
        except Exception as e:  # keep running; live metrics still work
            print(f"s3 flush failed: {e}", flush=True)


def main():
    start_http_server(8000)
    threading.Thread(target=_flush_loop, daemon=True).start()
    # Kafka is usually not accepting connections yet when this starts, and
    # KafkaConsumer raises NoBrokersAvailable immediately rather than retrying, so the
    # pod crash-looped twice on a live run before Kafka came up. Nothing was lost (the
    # topic keeps the events), but the restarts look like a fault and the exporter is
    # missing for however long the backoff lasts. Wait for the broker instead.
    consumer = None
    deadline = time.time() + int(os.environ.get("KAFKA_WAIT_SECONDS", "300"))
    while consumer is None:
        try:
            consumer = _consumer()
        except NoBrokersAvailable:
            if time.time() > deadline:
                raise
            print(f"waiting for kafka at {BOOTSTRAP}", flush=True)
            time.sleep(5)
    for msg in consumer:
        _handle(msg)


def _consumer():
    return KafkaConsumer(
        TOPIC, bootstrap_servers=BOOTSTRAP.split(","),
        group_id="latency-exporter", auto_offset_reset="latest",
        # TOMBSTONE-SAFE. The Flink gold emitters use upsert-kafka (the only Kafka sink
        # that accepts the updating stream a GROUP BY produces), and upsert-kafka writes
        # a NULL value on retraction. `b.decode` on None raises AttributeError inside the
        # deserializer, which kills the consumer loop and takes the whole exporter down, 
        # so every pipeline's metrics stop, not just the one that retracted.
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
    )
def _handle(msg):
    e = msg.value
    if not e:                      # tombstone from an upsert-kafka retraction
        return
    pipeline = e.get("pipeline", "unknown")
    delay_ms = e.get("delay_ms")
    if delay_ms is None and e.get("ingest_ts_ms") and e.get("executed_at_ms"):
        delay_ms = e["ingest_ts_ms"] - e["executed_at_ms"]
    if delay_ms is None:
        return
    DELAY.labels(pipeline).observe(max(delay_ms, 0) / 1000.0)
    EVENTS.labels(pipeline).inc()
    with _lock:
        _buf.append({"run_id": RUN_ID, "pipeline": pipeline,
                     "executed_at_ms": e.get("executed_at_ms"),
                     "ingest_ts_ms": e.get("ingest_ts_ms"),
                     "delay_ms": int(delay_ms),
                     # HOW THIS POINT WAS SAMPLED. Both families now sample the same
                     # records (trade_id % 997, or account_id % 97 on gold), but they
                     # emit differently: "flink_record" is one uniformly-sampled
                     # record, "spark_batch_extreme" is the oldest or newest sampled
                     # event of a committed batch. Percentiles over a pooled mix of
                     # the two are not a percentile of anything, split on this
                     # column before computing them offline. The live Grafana
                     # histogram deliberately pools: it is a freshness signal, not a
                     # published figure.
                     "sample_kind": e.get("sample_kind", "unknown"),
                     "ts_ms": int(time.time() * 1000)})


if __name__ == "__main__":
    main()
