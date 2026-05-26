"""
Iceberg query helpers for the Streamlit UI.

Uses PyIceberg (REST catalog) → Arrow → pandas.
Avoids needing Spark or Trino in the UI container.

Key notes:
- Call tbl.refresh() on every poll — PyIceberg caches metadata and won't see
  new commits without it.
- PyIceberg needs S3 credentials as catalog properties, not env vars alone.
- Three processing delay metrics are exposed because they tell different stories:
    end_to_end:  now - event_ts  (Postgres commit → UI visible)
    commit_lag:  commit_ts - event_ts  (engine processing + Iceberg commit cost)
    freshness:   now - max(commit_ts)  (how stale is the table regardless of input)
"""

import logging
import pandas as pd
import pyarrow.compute as pc
from datetime import timezone
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import In

import config
import kafka_cache

log = logging.getLogger(__name__)


def _make_catalog():
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": config.ICEBERG_REST_URI,
            "warehouse": config.ICEBERG_WAREHOUSE,
            "s3.endpoint": config.MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.access-key-id": config.AWS_ACCESS_KEY,
            "s3.secret-access-key": config.AWS_SECRET_KEY,
        }
    )


# Module-level catalog — reused across refreshes
_catalog = None


def get_catalog():
    global _catalog
    if _catalog is None:
        _catalog = _make_catalog()
    return _catalog


def load_table(namespace: str, table_name: str):
    cat = get_catalog()
    tbl = cat.load_table(f"{namespace}.{table_name}")
    tbl.refresh()  # force metadata reload to pick up new snapshots
    return tbl


def inventory_for_items(engine: str, item_ids: list[int]) -> pd.DataFrame:
    """Return current silver inventory rows for the given item IDs."""
    try:
        tbl = load_table("silver", f"item_inventory_{engine}")
        arrow = tbl.scan(row_filter=In("item_id", item_ids)).to_arrow()
        df = arrow.to_pandas()
        if df.empty:
            return pd.DataFrame(columns=["item_id", "qty_on_hand", "location", "event_ts", "commit_ts"])
        return (
            df[["item_id", "qty_on_hand", "location", "event_ts", "commit_ts"]]
            .sort_values("item_id")
            .reset_index(drop=True)
        )
    except Exception as e:
        log.warning(f"inventory_for_items({engine}): {e}")
        return pd.DataFrame({"error": [str(e)]})


def processing_delays(engine: str) -> dict:
    """
    Returns three delay metrics (seconds) over the most recent 2000 rows.
    """
    try:
        tbl = load_table("silver", f"item_inventory_{engine}")
        df = tbl.scan(limit=2000).to_arrow().to_pandas()
        if df.empty:
            return {"end_to_end_p50": None, "end_to_end_p95": None,
                    "commit_lag_p50": None,  "commit_lag_p95": None,
                    "freshness_s": None, "row_count": 0}

        now = pd.Timestamp.now(tz=timezone.utc)

        def to_utc(series):
            s = pd.to_datetime(series, utc=True, errors="coerce")
            return s

        event_ts  = to_utc(df["event_ts"])
        commit_ts = to_utc(df["commit_ts"])

        end_to_end = (now - event_ts).dt.total_seconds().dropna()
        commit_lag = (commit_ts - event_ts).dt.total_seconds().dropna()
        freshness  = (now - commit_ts.max()).total_seconds() if not commit_ts.isna().all() else None

        return {
            "end_to_end_p50": round(end_to_end.quantile(0.50), 1) if len(end_to_end) else None,
            "end_to_end_p95": round(end_to_end.quantile(0.95), 1) if len(end_to_end) else None,
            "commit_lag_p50": round(commit_lag.quantile(0.50), 1) if len(commit_lag) else None,
            "commit_lag_p95": round(commit_lag.quantile(0.95), 1) if len(commit_lag) else None,
            "freshness_s":    round(freshness, 1) if freshness is not None else None,
            "row_count": len(df),
        }
    except Exception as e:
        log.warning(f"processing_delays({engine}): {e}")
        return {"error": str(e)}


def kafka_latest_inventory(item_ids: list[int]) -> pd.DataFrame:
    """
    Return the latest CDC state per item_id from the Kafka consumer cache.
    This is the reconciliation reference — what the pipeline received from
    Debezium, without touching Postgres.
    """
    snapshots = kafka_cache.get_inventory_snapshot(item_ids)
    rows = []
    for iid in sorted(item_ids):
        snap = snapshots.get(iid)
        if snap is None:
            rows.append({"item_id": iid, "kafka_qty": None, "location": None, "op": None})
        else:
            rows.append({
                "item_id":   snap.item_id,
                "kafka_qty": snap.qty_on_hand,
                "location":  snap.location,
                "op":        snap.op,
            })
    return pd.DataFrame(rows)


def reconciliation_table(item_ids: list[int]) -> pd.DataFrame:
    """
    Build a side-by-side comparison:
        Kafka latest value (reference) vs Spark silver vs Flink silver.

    The Kafka cache holds the latest CDC event the pipeline received.
    Comparing silver against it answers: "did the merge produce the right result?"
    rather than "has the pipeline caught up to Postgres yet?" (a different question).

    Drift = silver qty differs from the latest Kafka-committed value.
    After pipeline lag settles (~20-30s), non-zero drift = merge correctness issue.

    Returns columns:
        item_id, kafka_qty, spark_qty, flink_qty,
        spark_drift, flink_drift, spark_ok, flink_ok
    """
    kafka = kafka_latest_inventory(item_ids)
    spark = inventory_for_items("spark", item_ids)
    flink = inventory_for_items("flink", item_ids)

    def _qty_map(df, col="qty_on_hand"):
        if "error" in df.columns or df.empty:
            return {}
        return dict(zip(df["item_id"].astype(int), df[col]))

    kafka_qty = _qty_map(kafka, "kafka_qty")
    spark_qty = _qty_map(spark)
    flink_qty = _qty_map(flink)

    tol = config.RECON_QTY_TOLERANCE
    rows = []
    for iid in sorted(item_ids):
        kv = kafka_qty.get(iid)
        sv = spark_qty.get(iid)
        fv = flink_qty.get(iid)

        sp_drift = abs(sv - kv) if kv is not None and sv is not None else None
        fl_drift = abs(fv - kv) if kv is not None and fv is not None else None

        rows.append({
            "item_id":     iid,
            "kafka_qty":   kv,
            "spark_qty":   sv,
            "flink_qty":   fv,
            "spark_drift": sp_drift,
            "flink_drift": fl_drift,
            "spark_ok":    (sp_drift is None) or (sp_drift <= tol),
            "flink_ok":    (fl_drift is None) or (fl_drift <= tol),
        })

    return pd.DataFrame(rows)


def snapshot_stats(engine: str, layer: str = "silver") -> dict:
    """
    Returns Iceberg snapshot and file-count stats for a given engine's silver table.
    """
    try:
        tbl = load_table(layer, f"item_inventory_{engine}")

        snapshots  = tbl.inspect.snapshots().to_pydict()
        snap_count = len(snapshots.get("snapshot_id", []))

        files_arrow = tbl.inspect.files()
        files_df    = files_arrow.to_pandas()

        data_files   = files_df[files_df["content"] == 0] if "content" in files_df.columns else files_df
        delete_files = files_df[files_df["content"] != 0] if "content" in files_df.columns else pd.DataFrame()

        total_size = int(files_df["file_size_in_bytes"].sum()) if "file_size_in_bytes" in files_df.columns else 0

        return {
            "snapshot_count":      snap_count,
            "data_file_count":     len(data_files),
            "delete_file_count":   len(delete_files),
            "total_size_mb":       round(total_size / 1024 / 1024, 2),
        }
    except Exception as e:
        log.warning(f"snapshot_stats({engine}, {layer}): {e}")
        return {"error": str(e)}
