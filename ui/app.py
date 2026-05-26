"""
Streamlit reporting UI: Spark vs Flink Iceberg silver table comparison.

Refreshes every 30 seconds. Shows:
  1. Item inventory side-by-side (5 selected items, Spark vs Flink silver)
  2. Processing delay metrics (end-to-end, commit lag, freshness)
  3. Iceberg snapshot & file stats (data files, delete files, size)
"""

import time
import streamlit as st
import pandas as pd

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import queries
import config
import kafka_cache

# Start Kafka consumer cache in background thread (replays topic history on startup,
# then stays live). This is the reconciliation reference — no Postgres queries.
kafka_cache.start()

st.set_page_config(
    page_title="Spark vs Flink — Iceberg Streaming",
    page_icon="⚡",
    layout="wide",
)

st.title("Spark vs Flink — Iceberg Silver Table Comparison")
st.caption(f"Auto-refreshes every {config.REFRESH_INTERVAL_SECS}s · Tracking items: {config.SELECTED_ITEM_IDS}")

# Helper: colour reconciliation rows red/green
def _style_recon(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    def row_style(row):
        spark_bad = not row.get("spark_ok", True)
        flink_bad = not row.get("flink_ok", True)
        styles = [""] * len(row)
        cols = list(row.index)
        if spark_bad and "spark_qty" in cols:
            styles[cols.index("spark_qty")]  = "background-color: #ffcccc"
            styles[cols.index("spark_drift")] = "background-color: #ffcccc"
        elif "spark_qty" in cols and row.get("spark_qty") is not None:
            styles[cols.index("spark_qty")]  = "background-color: #ccffcc"
            styles[cols.index("spark_drift")] = "background-color: #ccffcc"
        if flink_bad and "flink_qty" in cols:
            styles[cols.index("flink_qty")]  = "background-color: #ffcccc"
            styles[cols.index("flink_drift")] = "background-color: #ffcccc"
        elif "flink_qty" in cols and row.get("flink_qty") is not None:
            styles[cols.index("flink_qty")]  = "background-color: #ccffcc"
            styles[cols.index("flink_drift")] = "background-color: #ccffcc"
        return styles
    display = df.drop(columns=["spark_ok", "flink_ok"], errors="ignore")
    return display.style.apply(row_style, axis=1)

placeholder = st.empty()

while True:
    with placeholder.container():

        # ── Section 1: Inventory tables ──────────────────────────────────────
        st.subheader("Current Inventory (Silver Tables)")
        col_spark, col_flink = st.columns(2)

        with col_spark:
            st.markdown("**Spark Silver**")
            df_spark = queries.inventory_for_items("spark", config.SELECTED_ITEM_IDS)
            st.dataframe(df_spark, use_container_width=True)

        with col_flink:
            st.markdown("**Flink Silver**")
            df_flink = queries.inventory_for_items("flink", config.SELECTED_ITEM_IDS)
            st.dataframe(df_flink, use_container_width=True)

        st.divider()

        # ── Section 2: Reconciliation ─────────────────────────────────────────
        st.subheader("Reconciliation — Kafka latest vs Silver Tables")

        if not kafka_cache.is_ready():
            st.info("Kafka cache replaying topic history from offset 0 — check back in a few seconds...")
        elif kafka_cache.cache_error():
            st.warning(f"Kafka cache error: {kafka_cache.cache_error()}")
        else:
            st.caption(
                f"Reference is the **Kafka consumer cache** — latest CDC event received per item "
                f"({kafka_cache.messages_consumed():,} messages consumed). "
                "No Postgres queries. "
                "**Green** = silver matches Kafka latest. **Red** = drift. "
                "Non-zero drift after pipeline lag settles (~20–30s) = merge correctness issue."
            )

            recon_df = queries.reconciliation_table(config.SELECTED_ITEM_IDS)

            if "error" in recon_df.columns:
                st.warning(f"Reconciliation unavailable: {recon_df.iloc[0]['error']}")
            else:
                mismatch_count = int((~recon_df["spark_ok"]).sum() + (~recon_df["flink_ok"]).sum())
                if mismatch_count > 0:
                    st.error(f"{mismatch_count} mismatch(es) detected — check for merge correctness issues")
                else:
                    st.success("All tracked items match Kafka latest (within tolerance)")

                st.dataframe(_style_recon(recon_df), use_container_width=True)

        st.divider()

        # ── Section 4: Processing delay ───────────────────────────────────────
        st.subheader("Processing Delay")
        st.caption(
            "**End-to-end**: now − event_ts (Postgres → visible in silver)  |  "
            "**Commit lag**: commit_ts − event_ts (engine processing cost)  |  "
            "**Freshness**: now − max(commit_ts) (table staleness)"
        )

        d_spark = queries.processing_delays("spark")
        d_flink = queries.processing_delays("flink")

        m1, m2, m3, m4, m5, m6 = st.columns(6)

        def _fmt(v):
            return f"{v}s" if v is not None else "—"

        m1.metric("Spark e2e p50",      _fmt(d_spark.get("end_to_end_p50")))
        m2.metric("Spark e2e p95",      _fmt(d_spark.get("end_to_end_p95")))
        m3.metric("Spark freshness",    _fmt(d_spark.get("freshness_s")))
        m4.metric("Flink e2e p50",      _fmt(d_flink.get("end_to_end_p50")))
        m5.metric("Flink e2e p95",      _fmt(d_flink.get("end_to_end_p95")))
        m6.metric("Flink freshness",    _fmt(d_flink.get("freshness_s")))

        # Commit lag detail in an expander
        with st.expander("Commit lag detail"):
            lag_col1, lag_col2 = st.columns(2)
            lag_col1.metric("Spark commit lag p50",  _fmt(d_spark.get("commit_lag_p50")))
            lag_col1.metric("Spark commit lag p95",  _fmt(d_spark.get("commit_lag_p95")))
            lag_col2.metric("Flink commit lag p50",  _fmt(d_flink.get("commit_lag_p50")))
            lag_col2.metric("Flink commit lag p95",  _fmt(d_flink.get("commit_lag_p95")))

        st.divider()

        # ── Section 5: Iceberg snapshot stats ────────────────────────────────
        st.subheader("Iceberg Snapshot & File Stats (item_inventory silver)")

        s_spark = queries.snapshot_stats("spark")
        s_flink = queries.snapshot_stats("flink")

        stats_col1, stats_col2 = st.columns(2)

        with stats_col1:
            st.markdown("**Spark Silver**")
            if "error" in s_spark:
                st.warning(s_spark["error"])
            else:
                st.metric("Snapshots",          s_spark.get("snapshot_count",  "—"))
                st.metric("Data files",         s_spark.get("data_file_count", "—"))
                st.metric("Delete files",       s_spark.get("delete_file_count","—"))
                st.metric("Total size (MB)",    s_spark.get("total_size_mb",   "—"))

        with stats_col2:
            st.markdown("**Flink Silver**")
            if "error" in s_flink:
                st.warning(s_flink["error"])
            else:
                st.metric("Snapshots",          s_flink.get("snapshot_count",  "—"))
                st.metric("Data files",         s_flink.get("data_file_count", "—"))
                st.metric("Delete files",       s_flink.get("delete_file_count","—"))
                st.metric("Total size (MB)",    s_flink.get("total_size_mb",   "—"))

        st.divider()
        st.caption(f"Last refreshed: {pd.Timestamp.now().strftime('%H:%M:%S')}")

    time.sleep(config.REFRESH_INTERVAL_SECS)
    st.rerun()
