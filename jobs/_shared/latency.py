"""
Per-pipeline latency emit, feeds the `pipeline_latency` topic that
docker/latency-exporter consumes for the "Pipeline Comparison. Live" dashboard.

SAMPLING. THE SAME RECORDS ON ALL FIVE ENGINES.
Every engine emits for exactly one population: trades where trade_id % 997 == 0 on the
bronze and silver hops, and account_id % 97 == 0 on gold (gold rows are per account, so
a trade key does not exist there). The Flink jobs express it as a WHERE MOD(...) = 0;
these observe() aggregates are restricted to the same predicate. Before this the Spark
side sampled NOTHING (it aggregated the whole batch) so a Spark point and a Flink
point were not measurements of the same thing and the two could not be compared.

WHAT IS STILL NOT SYMMETRIC, and cannot be made so cheaply: Flink emits one message per
sampled RECORD; Spark emits per committed BATCH, because observe() computes aggregates
and a per-row emit needs a second Spark action over the batch (see below, that version
existed and roughly doubled the job's work). So Spark contributes the OLDEST and NEWEST
sampled event of each batch, which brackets that batch's real lag; emitting only the max,
as this did before, reported the batch's best case and nothing else. Each message carries
`sample_kind` so a consumer can tell the two apart instead of pooling them by accident.

The headline cross-engine number is not this topic. It is
(commit_ts - last_updated_at) read straight out of gold.open_positions, every row, no
sampling, identical definition on all five engines (infra/aws/scripts/snapshot-results.sh,
processing_delay.csv). This topic decomposes delay per HOP, which that metric cannot do.

APPROACH: Dataset.observe() + StreamingQueryListener.

`observe()` attaches a named aggregate to the query, computed as part of the pass
Spark is ALREADY making over the batch, no extra action, no recomputation. The
listener's onQueryProgress then fires once the batch has COMMITTED, and reads those
metrics off the progress event.

This replaced a foreachBatch that wrote sampled rows to Kafka. That version took a
SECOND Spark action on an uncached batch_df, so every batch re-ran the Kafka read and
JSON parse purely to emit telemetry, roughly doubling the work in the job whose
latency is being measured. It also forced the native streaming sink to be
restructured. observe() has neither problem: the sink stays
`.format("delta").start()` exactly as it was.

WHERE THE CLOCK STOPS
  event time  : max(executed_at) observed during the batch, the newest SOURCE event
  commit time : when onQueryProgress fires, i.e. after the batch is committed
  delay       = commit time − observed max event time
Commit is the meaningful boundary: Delta compacts inline, Paimon self-compacts, Hudi
does inline MOR compaction, Iceberg defers to a rewrite job. Measuring during
processing would hide exactly those differences.

Emitting from the listener uses a plain Kafka producer, not Spark, the listener runs
on the driver and a Spark action inside it risks deadlock.

Every failure is swallowed and logged: telemetry must never break a pipeline.
"""
import json
import os
import time

OBSERVE_NAME = "pipeline_latency_obs"

# Sample rate and key, PER HOP. These must stay in step with the MOD(...) predicates in
# the Flink SQL (jobs/flink-*/{bronze,silver}_trades.sql use 997 on trade_id,
# gold_open_positions.sql uses 97 on account_id), if they drift, the families stop
# measuring the same records and the per-hop comparison quietly becomes meaningless.
SAMPLE_TRADES = ("trade_id", int(os.environ.get("LATENCY_SAMPLE_MOD_TRADES", "997")))
SAMPLE_ACCOUNTS = ("account_id", int(os.environ.get("LATENCY_SAMPLE_MOD_ACCOUNTS", "97")))

_TOPIC = os.environ.get("LATENCY_TOPIC", "pipeline_latency")
_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
_ENABLED = os.environ.get("LATENCY_EMIT_ENABLED", "true").lower() != "false"


def observe_event_time(df, event_time_col="executed_at", sample=SAMPLE_TRADES):
    """Attach the observation to the streaming DataFrame. Costs no extra action.

    `sample` is the (column, modulus) pair identifying the sampled population, pass
    SAMPLE_ACCOUNTS on the gold hop, where the Flink side keys on account_id.
    """
    from pyspark.sql.functions import col, count as _count, max as _max, min as _min, when
    sample_col, sample_mod = sample
    # Restricted to the sampled rows, so this observes the same records the Flink jobs
    # emit. `when` returns null off-sample and min/max/count all skip nulls.
    picked = when(col(sample_col) % sample_mod == 0, col(event_time_col))
    return df.observe(
        OBSERVE_NAME,
        _max(picked).alias("max_event_ts"),
        _min(picked).alias("min_event_ts"),
        _count(picked).alias("rows"),
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
            # Fires AFTER the batch commits, this is the measurement point.
            try:
                obs = (event.progress.observedMetrics or {}).get(OBSERVE_NAME)
                if obs is None:
                    return
                if not obs["rows"]:
                    return               # no SAMPLED row in this batch: nothing to time
                commit_ms = int(time.time() * 1000)
                # Oldest and newest sampled event: the batch's worst and best lag. One
                # point would have to be one or the other, and reporting only the newest
                # (what this did before) understates every batch it describes.
                for ev in (obs["min_event_ts"], obs["max_event_ts"]):
                    if ev is None:
                        continue
                    self._kafka().send(_TOPIC, {
                        "pipeline": pipeline,
                        "executed_at_ms": int(ev.timestamp() * 1000),
                        "ingest_ts_ms": commit_ms,
                        "sample_kind": "spark_batch_extreme",
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
