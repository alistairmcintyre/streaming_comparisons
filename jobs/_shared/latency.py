"""
Per-pipeline latency emit — feeds the `pipeline_latency` topic that
docker/latency-exporter consumes for the "Pipeline Comparison — Live" dashboard.

APPROACH: Dataset.observe() + StreamingQueryListener.

`observe()` attaches a named aggregate to the query, computed as part of the pass
Spark is ALREADY making over the batch — no extra action, no recomputation. The
listener's onQueryProgress then fires once the batch has COMMITTED, and reads those
metrics off the progress event.

This replaced a foreachBatch that wrote sampled rows to Kafka. That version took a
SECOND Spark action on an uncached batch_df, so every batch re-ran the Kafka read and
JSON parse purely to emit telemetry — roughly doubling the work in the job whose
latency is being measured. It also forced the native streaming sink to be
restructured. observe() has neither problem: the sink stays
`.format("delta").start()` exactly as it was.

WHERE THE CLOCK STOPS
  event time  : max(executed_at) observed during the batch — the newest SOURCE event
  commit time : when onQueryProgress fires, i.e. after the batch is committed
  delay       = commit time − observed max event time
Commit is the meaningful boundary: Delta compacts inline, Paimon self-compacts, Hudi
does inline MOR compaction, Iceberg defers to a rewrite job. Measuring during
processing would hide exactly those differences.

Emitting from the listener uses a plain Kafka producer, NOT Spark — the listener runs
on the driver and a Spark action inside it risks deadlock.

Every failure is swallowed and logged: telemetry must never break a pipeline.
"""
import json
import os
import time

OBSERVE_NAME = "pipeline_latency_obs"

_TOPIC = os.environ.get("LATENCY_TOPIC", "pipeline_latency")
_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
_ENABLED = os.environ.get("LATENCY_EMIT_ENABLED", "true").lower() != "false"


def observe_event_time(df, event_time_col="executed_at"):
    """Attach the observation to the streaming DataFrame. Costs no extra action."""
    from pyspark.sql.functions import max as _max, count as _count, col
    return df.observe(
        OBSERVE_NAME,
        _max(col(event_time_col)).alias("max_event_ts"),
        _count(col(event_time_col)).alias("rows"),
    )


def _make_listener(pipeline):
    from pyspark.sql.streaming import StreamingQueryListener

    class _LatencyListener(StreamingQueryListener):
        def __init__(self):
            self._producer = None

        def _kafka(self):
            if self._producer is None:
                from kafka import KafkaProducer
                self._producer = KafkaProducer(
                    bootstrap_servers=_BROKERS.split(","),
                    value_serializer=lambda v: json.dumps(v).encode(),
                    linger_ms=200, retries=2,
                )
            return self._producer

        def onQueryStarted(self, event):
            pass

        def onQueryTerminated(self, event):
            pass

        def onQueryProgress(self, event):
            # Fires AFTER the batch commits — this is the measurement point.
            try:
                obs = (event.progress.observedMetrics or {}).get(OBSERVE_NAME)
                if obs is None:
                    return
                max_ev = obs["max_event_ts"]
                if max_ev is None or not obs["rows"]:
                    return                       # empty batch: nothing to time
                commit_ms = int(time.time() * 1000)
                event_ms = int(max_ev.timestamp() * 1000)
                self._kafka().send(_TOPIC, {
                    "pipeline": pipeline,
                    "executed_at_ms": event_ms,
                    "ingest_ts_ms": commit_ms,
                })
            except Exception as e:
                print(f"[latency] emit failed for {pipeline}: {str(e)[:160]}", flush=True)

    return _LatencyListener()


def attach_latency_listener(spark, pipeline):
    """Register the post-commit emitter. Call once, before .start()."""
    if not _ENABLED:
        return
    try:
        spark.streams.addListener(_make_listener(pipeline))
        print(f"[latency] listener attached for {pipeline}", flush=True)
    except Exception as e:
        print(f"[latency] could not attach listener: {str(e)[:160]}", flush=True)
