"""
Per-pipeline latency emit — feeds the `pipeline_latency` topic that
docker/latency-exporter consumes for the "Pipeline Comparison — Live" dashboard.

WHERE THE CLOCK STOPS, AND WHY IT MATTERS
Emit happens AFTER the engine's write call returns, i.e. after the batch is
COMMITTED and queryable — not while records are being processed. Commit is where the
engines actually differ: Delta compacts inline, Paimon self-compacts in the writer,
Hudi does inline MOR compaction, Iceberg defers to a separate rewrite job. Measuring
at processing time would hide precisely the differences this benchmark exists to
show.

Event shape (what exporter.py expects):
    {"pipeline": "<engine>-<layer>", "executed_at_ms": <source event>, "ingest_ts_ms": <commit>}
delay = ingest_ts_ms - executed_at_ms  → source event to queryable-in-lake.

SAMPLED, deliberately. At 1k/s across five engines, emitting every record would add
5k msg/s of measurement traffic to the very Kafka the pipelines are reading — the
observer changing the thing observed. LATENCY_SAMPLE_N takes 1 row per batch by
default, which at a 10s trigger is ~6 points/minute/pipeline: plenty for
histogram_quantile, negligible load.

Never let telemetry break a pipeline: every failure here is swallowed and logged.
"""
import os
import time

_TOPIC = os.environ.get("LATENCY_TOPIC", "pipeline_latency")
_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
_SAMPLE_N = int(os.environ.get("LATENCY_SAMPLE_N", "1"))
_ENABLED = os.environ.get("LATENCY_EMIT_ENABLED", "true").lower() != "false"


def emit_commit_latency(batch_df, pipeline, event_time_col="executed_at"):
    """Call AFTER the write returns, inside foreachBatch. batch_df is the rows just
    committed; `event_time_col` is the SOURCE event time carried on each row."""
    if not _ENABLED:
        return
    try:
        from pyspark.sql.functions import col, lit, to_json, struct
        commit_ms = int(time.time() * 1000)          # the write has returned = committed
        sample = batch_df.select(col(event_time_col).alias("_ev")).limit(_SAMPLE_N)
        payload = (sample
                   .filter(col("_ev").isNotNull())
                   .select(to_json(struct(
                       lit(pipeline).alias("pipeline"),
                       (col("_ev").cast("double") * 1000).cast("long").alias("executed_at_ms"),
                       lit(commit_ms).alias("ingest_ts_ms"),
                   )).alias("value")))
        (payload.write.format("kafka")
            .option("kafka.bootstrap.servers", _BROKERS)
            .option("topic", _TOPIC)
            .save())
    except Exception as e:                            # telemetry must never break the pipeline
        print(f"[latency] emit failed for {pipeline}: {str(e)[:160]}", flush=True)
