"""Live operations dashboard for the QuickBite lakehouse.

Reads the Delta tables directly with delta-rs (no Spark needed) plus the
streaming-metrics JSONL files, and refreshes every 10 seconds.

Run:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import streamlit as st
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

LAKE = Path(os.getenv("LAKE_ROOT", "data")) / "lake"
METRICS = Path(os.getenv("METRICS_DIR", Path(os.getenv("LAKE_ROOT", "data")) / "metrics"))
MANIFEST = Path(os.getenv("MANIFEST_PATH", Path(os.getenv("LAKE_ROOT", "data")) / "generator/manifest.json"))

# Validated categorical order (fixed, never cycled) and chart chrome.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
SINGLE = SERIES[0]
CRITICAL = "#d03b3b"

st.set_page_config(page_title="QuickBite live ops", layout="wide")


# ------------------------------------------------------------------ loading
def _utcnow() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))


@st.cache_data(ttl=5, show_spinner=False)
def load(table: str, columns: tuple[str, ...] | None = None, since_col: str | None = None,
         since_minutes: int | None = None) -> pd.DataFrame:
    path = LAKE / table
    try:
        dt = DeltaTable(str(path))
    except TableNotFoundError:
        return pd.DataFrame(columns=list(columns or []))
    dataset = dt.to_pyarrow_dataset()
    flt = None
    if since_col and since_minutes:
        col_type = dataset.schema.field(since_col).type
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        if getattr(col_type, "tz", None) is None:
            cutoff = cutoff.replace(tzinfo=None)
        flt = pc.field(since_col) >= pa.scalar(cutoff, type=col_type)
    df = dataset.to_table(columns=list(columns) if columns else None, filter=flt).to_pandas()
    # Normalise every timestamp to naive UTC so arithmetic is uniform.
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


@st.cache_data(ttl=5, show_spinner=False)
def load_metrics(tail: int = 400) -> pd.DataFrame:
    rows = []
    for f in sorted(METRICS.glob("*.jsonl")):
        lines = f.read_text().splitlines()[-tail:]
        rows.extend(json.loads(line) for line in lines if line.strip())
    df = pd.DataFrame(rows)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_localize(None)
    return df


def fmt_int(n) -> str:
    return f"{int(n):,}" if pd.notna(n) else "-"


def fmt_inr(n) -> str:
    return f"₹{float(n):,.0f}" if pd.notna(n) else "-"


# -------------------------------------------------------------------- layout
st.title("QuickBite · real-time order operations")
st.caption("Kafka → Spark Structured Streaming → Delta Lake (bronze / silver / gold). Refreshes every 10 s.")

window_min = st.segmented_control(
    "Time range", options=[15, 30, 60, 240], default=30, format_func=lambda m: f"Last {m} min"
) or 30


@st.fragment(run_every="10s")
def live(window_min: int) -> None:
    now = _utcnow()
    zone = load("gold/zone_metrics_1m", since_col="window_start", since_minutes=window_min)
    alerts = load("gold/sla_alerts", since_col="detected_at", since_minutes=window_min)
    conv = load("gold/checkout_conversion", since_col="checkout_ts", since_minutes=window_min)
    current = load("silver/orders_current", ("status", "zone", "last_event_ts"))
    lat = load("silver/order_events", ("kafka_ts", "silver_processed_at"),
               since_col="silver_processed_at", since_minutes=5)
    dlq = load("silver/dead_letter", ("source_topic", "primary_reason", "detected_at"),
               since_col="detected_at", since_minutes=window_min)
    metrics = load_metrics()

    # ---------------------------------------------------------- KPI tiles
    breaches = alerts[alerts["alert_type"] == "SLA_BREACH"] if not alerts.empty else alerts
    resolved = set(alerts.loc[alerts["alert_type"] == "DELIVERED_AFTER_BREACH", "order_id"]) if not alerts.empty else set()
    open_breaches = breaches[~breaches["order_id"].isin(resolved)] if not breaches.empty else breaches
    conv_rate = conv["converted"].mean() if not conv.empty else None
    e2e = (lat["silver_processed_at"] - lat["kafka_ts"]).dt.total_seconds() if not lat.empty else pd.Series(dtype=float)

    c = st.columns(6)
    c[0].metric("Orders placed", fmt_int(zone["orders_placed"].sum() if not zone.empty else 0))
    c[1].metric("GMV", fmt_inr(zone["gmv"].astype(float).sum() if not zone.empty else 0))
    c[2].metric("Open SLA breaches", fmt_int(len(open_breaches)))
    c[3].metric("Checkout → order", f"{conv_rate:.0%}" if conv_rate is not None else "-")
    c[4].metric("Kafka → silver p95", f"{e2e.quantile(0.95):.1f} s" if len(e2e) else "-",
                help="Broker timestamp to silver commit, last 5 minutes")
    c[5].metric("Dead-lettered", fmt_int(len(dlq)))

    left, right = st.columns([3, 2])

    # ------------------------------------------------ orders per minute
    with left:
        st.subheader("Orders per minute")
        if zone.empty:
            st.info("Waiting for the first finalised 1-minute window (watermark + 1 min after start).")
        else:
            per_min = zone.groupby("window_start", as_index=False).agg(
                orders=("orders_placed", "sum"), gmv=("gmv", lambda s: float(s.astype(float).sum())))
            st.altair_chart(
                alt.Chart(per_min).mark_line(color=SINGLE, strokeWidth=2, point=alt.OverlayMarkDef(size=40, color=SINGLE))
                .encode(
                    x=alt.X("window_start:T", title=None),
                    y=alt.Y("orders:Q", title="Orders placed"),
                    tooltip=[alt.Tooltip("window_start:T", title="Minute", format="%H:%M"),
                             alt.Tooltip("orders:Q", title="Orders"),
                             alt.Tooltip("gmv:Q", title="GMV (₹)", format=",.0f")],
                ).properties(height=260),
                use_container_width=True,
            )

    # ------------------------------------------------ orders by zone
    with right:
        st.subheader("Orders by zone")
        if not zone.empty:
            by_zone = zone.groupby("zone", as_index=False)["orders_placed"].sum()
            st.altair_chart(
                alt.Chart(by_zone).mark_bar(color=SINGLE, cornerRadiusEnd=4, size=14)
                .encode(
                    y=alt.Y("zone:N", sort="-x", title=None),
                    x=alt.X("orders_placed:Q", title="Orders placed"),
                    tooltip=[alt.Tooltip("zone:N", title="Zone"), alt.Tooltip("orders_placed:Q", title="Orders")],
                ).properties(height=260),
                use_container_width=True,
            )

    left, right = st.columns([3, 2])

    # ------------------------------------------------ SLA alerts
    with left:
        st.subheader("SLA breaches (live)")
        if open_breaches.empty:
            st.success("No open SLA breaches.")
        else:
            show = open_breaches.sort_values("detected_at", ascending=False).head(15).copy()
            show["open_min"] = (show["open_seconds"] / 60).round(1)
            st.dataframe(
                show[["order_id", "zone", "restaurant_id", "last_status", "placed_ts", "detected_at", "open_min"]],
                hide_index=True, use_container_width=True,
                column_config={"open_min": st.column_config.NumberColumn("Open (min)", format="%.1f")},
            )

    # ------------------------------------------------ order funnel
    with right:
        st.subheader("Orders by current status")
        if not current.empty:
            order = ["PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED"]
            recent = current[current["last_event_ts"] >= now - timedelta(minutes=window_min)]
            counts = recent.groupby("status", as_index=False).size().rename(columns={"size": "orders"})
            st.altair_chart(
                alt.Chart(counts).mark_bar(color=SINGLE, cornerRadiusEnd=4, size=14)
                .encode(
                    y=alt.Y("status:N", sort=order, title=None),
                    x=alt.X("orders:Q", title="Orders"),
                    tooltip=["status", "orders"],
                ).properties(height=220),
                use_container_width=True,
            )

    # ------------------------------------------------ pipeline health
    st.subheader("Pipeline health")
    if metrics.empty:
        st.info("No streaming progress recorded yet.")
    else:
        recent_m = metrics[metrics["timestamp"] >= now - timedelta(minutes=window_min)]
        layers = ["bronze_ingest", "silver_orders", "silver_clickstream"]
        thr = recent_m[recent_m["query"].isin(layers)]
        h1, h2 = st.columns([3, 2])
        with h1:
            if not thr.empty:
                st.altair_chart(
                    alt.Chart(thr).mark_line(strokeWidth=2)
                    .encode(
                        x=alt.X("timestamp:T", title=None),
                        y=alt.Y("processed_rows_per_sec:Q", title="Rows / second processed"),
                        color=alt.Color("query:N", title="Query",
                                        scale=alt.Scale(domain=layers, range=SERIES),
                                        legend=alt.Legend(orient="top")),
                        tooltip=["query", alt.Tooltip("timestamp:T", format="%H:%M:%S"),
                                 "num_input_rows", "processed_rows_per_sec", "batch_duration_ms"],
                    ).properties(height=240),
                    use_container_width=True,
                )
        with h2:
            latest = (metrics.sort_values("timestamp").groupby("query").tail(1)
                      .set_index("query")[["batch_id", "batch_duration_ms", "state_rows", "watermark"]])
            dropped = recent_m.groupby("query")["rows_dropped_by_watermark"].sum().rename("dropped_late")
            st.dataframe(latest.join(dropped), use_container_width=True)
            st.caption("dropped_late = events that arrived after the watermark and were excluded "
                       "from windowed aggregates (they are still in silver).")

    # ------------------------------------------------ data quality
    st.subheader("Data quality: dead-letter queue")
    if dlq.empty:
        st.success("No rejected records in this window.")
    else:
        reasons = dlq.groupby(["source_topic", "primary_reason"], as_index=False).size()
        st.dataframe(reasons.rename(columns={"size": "records"}).sort_values("records", ascending=False),
                     hide_index=True, use_container_width=True)

    if MANIFEST.exists():
        with st.expander("Generator ground truth (last manifest)"):
            st.json(json.loads(MANIFEST.read_text()))


live(window_min)
