"""Gold transformations: the four streaming patterns this project showcases.

Each function takes streaming DataFrames and returns a streaming DataFrame.
They are wired to sinks in ``pipeline/jobs/gold.py``.

=====================  ===========================================  =====================
Query                  Pattern                                      Question it answers
=====================  ===========================================  =====================
zone_metrics_1m        tumbling event-time window + watermark       Orders & GMV per zone per minute
sla_alerts             arbitrary stateful processing + timers       Which orders are breaching SLA now?
checkout_conversion    stream-stream LEFT OUTER join, time-bounded  Did a checkout become an order?
user_sessions          session windows (gap-based)                  How do users browse before buying?
=====================  ===========================================  =====================
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from pipeline.state.order_sla import OUTPUT_SCHEMA, STATE_SCHEMA, spark_state_func


def zone_metrics_1m(order_events: DataFrame, watermark: str) -> DataFrame:
    """Per-zone, per-minute business metrics.

    Append mode + watermark: a window is emitted exactly once, when the
    watermark passes its end, so the Delta table never needs updates.
    Events later than the watermark are dropped from these aggregates (and
    counted as ``numRowsDroppedByWatermark`` in the metrics)."""
    placed = F.col("status") == "PLACED"
    return (
        order_events.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", "1 minute"), F.col("zone"))
        .agg(
            F.count(F.when(placed, 1)).alias("orders_placed"),
            F.sum(F.when(placed, F.col("amount"))).alias("gmv"),
            F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("orders_delivered"),
            F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("orders_cancelled"),
            # Exact distinct counts are not supported on streams; HyperLogLog is.
            F.approx_count_distinct(F.when(placed, F.col("customer_id"))).alias("unique_customers"),
            F.count(F.lit(1)).alias("events"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone",
            "orders_placed",
            F.coalesce("gmv", F.lit(0).cast("decimal(20,2)")).alias("gmv"),
            "orders_delivered",
            "orders_cancelled",
            "unique_customers",
            "events",
            F.current_timestamp().alias("emitted_at"),
        )
    )


def sla_alerts(order_events: DataFrame, watermark: str, sla_seconds: int, ttl_seconds: int) -> DataFrame:
    """Event-time timers per order. See pipeline/state/order_sla.py."""
    return (
        order_events.select("order_id", "status", "event_ts", "zone", "restaurant_id")
        .withWatermark("event_ts", watermark)
        .groupBy("order_id")
        .applyInPandasWithState(
            spark_state_func(sla_seconds * 1000, ttl_seconds * 1000),
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
        .withColumn("emitted_at", F.current_timestamp())
    )


def checkout_conversion(
    clicks: DataFrame, order_events: DataFrame, watermark: str, window_seconds: int
) -> DataFrame:
    """Join each checkout to the order placed from the same session within
    ``window_seconds``. LEFT OUTER so abandoned checkouts are emitted too
    (with ``converted = false``) once the watermark proves no order can
    still arrive. Both sides need a watermark and the join needs a time
    bound, otherwise Spark would have to keep join state forever."""
    checkouts = (
        clicks.filter(F.col("event_type") == "checkout_started")
        .select(
            F.col("session_id").alias("c_session_id"),
            F.col("customer_id"),
            F.col("zone"),
            F.col("restaurant_id"),
            F.col("device"),
            F.col("event_ts").alias("checkout_ts"),
        )
        .withWatermark("checkout_ts", watermark)
    )
    placed = (
        order_events.filter(F.col("status") == "PLACED")
        .select(
            F.col("session_id").alias("o_session_id"),
            F.col("order_id"),
            F.col("amount"),
            F.col("event_ts").alias("order_ts"),
        )
        .withWatermark("order_ts", watermark)
    )
    joined = checkouts.join(
        placed,
        F.expr(
            f"""o_session_id = c_session_id
                AND order_ts >= checkout_ts
                AND order_ts <= checkout_ts + INTERVAL {window_seconds} SECONDS"""
        ),
        "leftOuter",
    )
    return joined.select(
        F.col("c_session_id").alias("session_id"),
        "customer_id",
        "zone",
        "restaurant_id",
        "device",
        "checkout_ts",
        "order_id",
        "amount",
        "order_ts",
        F.col("order_id").isNotNull().alias("converted"),
        (F.col("order_ts").cast("double") - F.col("checkout_ts").cast("double")).alias("seconds_to_order"),
        F.current_timestamp().alias("emitted_at"),
    )


def user_sessions(clicks: DataFrame, watermark: str, gap_seconds: int) -> DataFrame:
    """Sessionise raw clicks per customer by inactivity gap.

    We deliberately ignore the app's own session_id: this shows how to
    derive sessions when the source does not provide them (web logs, IoT)."""
    return (
        clicks.withWatermark("event_ts", watermark)
        .groupBy(F.session_window("event_ts", f"{gap_seconds} seconds"), F.col("customer_id"))
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
            F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).cast("boolean").alias("added_to_cart"),
            F.max(F.when(F.col("event_type") == "checkout_started", 1).otherwise(0)).cast("boolean").alias("reached_checkout"),
            F.approx_count_distinct("restaurant_id").alias("restaurants_viewed"),
            F.first("device", ignorenulls=True).alias("device"),
            F.first("zone", ignorenulls=True).alias("zone"),
        )
        .select(
            F.col("session_window.start").alias("session_start"),
            F.col("session_window.end").alias("session_end"),
            "customer_id",
            "zone",
            "device",
            "events",
            (F.col("last_event_ts").cast("double") - F.col("first_event_ts").cast("double")).alias("active_seconds"),
            "added_to_cart",
            "reached_checkout",
            "restaurants_viewed",
            F.current_timestamp().alias("emitted_at"),
        )
    )
