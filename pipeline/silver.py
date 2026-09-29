"""Silver transformations (pure functions over DataFrames + Delta writes).

Kept separate from the streaming job wiring so each step can be tested with a
plain static DataFrame.
"""

from __future__ import annotations

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.avro_decode import SchemaResolver, decode_confluent_avro
from pipeline.config import Paths
from pipeline.quality import apply_rules, click_rules, order_rules
from pipeline.schemas import CLICK_EVENT_JSON_SCHEMA, STATUS_RANK
from pipeline.tables import DEAD_LETTER, SILVER_CLICKSTREAM, SILVER_ORDER_EVENTS

log = logging.getLogger("pipeline.silver")


def status_rank_col(col: str = "status"):
    expr = F.lit(None).cast("int")
    for status, rank in STATUS_RANK.items():
        expr = F.when(F.col(col) == status, F.lit(rank)).otherwise(expr)
    return expr


# ---------------------------------------------------------------- dead letters
def to_dead_letter(df: DataFrame, stage: str) -> DataFrame:
    """Project rejected rows onto the dead-letter schema."""
    event_id = F.col("event_id") if "event_id" in df.columns else F.lit(None).cast("string")
    return df.select(
        F.col("topic").alias("source_topic"),
        F.lit(stage).alias("pipeline_stage"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_ts"),
        event_id.alias("event_id"),
        F.element_at("dq_errors", 1).alias("primary_reason"),
        F.col("dq_errors").alias("error_reasons"),
        F.col("value").alias("raw_value"),
        F.current_timestamp().alias("detected_at"),
        F.current_date().alias("detected_date"),
    ).select(*[F.col(f.name) for f in DEAD_LETTER.fields])


# ---------------------------------------------------------------------- orders
def decode_and_validate_orders(
    bronze_batch: DataFrame, resolver: SchemaResolver, max_future_skew_seconds: int
) -> DataFrame:
    decoded = decode_confluent_avro(bronze_batch, resolver)
    return apply_rules(decoded, order_rules(max_future_skew_seconds), "decode_error")


def to_silver_order_events(valid: DataFrame) -> DataFrame:
    """Shape valid rows for silver and drop duplicates *within* the batch.

    Cross-batch duplicates are handled by the insert-only MERGE."""
    shaped = (
        valid.withColumn("status_rank", status_rank_col())
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumnRenamed("ingested_at", "bronze_ingested_at")
        .withColumn("silver_processed_at", F.current_timestamp())
    )
    # Keep the copy that reached Kafka first (lowest offset) - deterministic.
    w = Window.partitionBy("event_id").orderBy("kafka_ts", "kafka_partition", "kafka_offset")
    return (
        shaped.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(*[F.col(f.name) for f in SILVER_ORDER_EVENTS.fields])
    )


def merge_order_events(spark: SparkSession, events: DataFrame, path: str) -> None:
    """Insert-only MERGE on event_id: idempotent, removes cross-batch duplicates.

    The event_date predicate lets Delta prune to the partitions this batch
    touches instead of scanning the whole table."""
    dates = [r["event_date"] for r in events.select("event_date").distinct().collect()]
    if not dates:
        return
    date_list = ", ".join(f"DATE'{d.isoformat()}'" for d in dates)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(
            events.alias("s"),
            f"t.event_date IN ({date_list}) AND t.event_date = s.event_date AND t.event_id = s.event_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def latest_per_order(events: DataFrame) -> DataFrame:
    """Collapse a batch to one row per order: the most advanced status, plus
    the earliest PLACED time seen in the batch."""
    w = Window.partitionBy("order_id").orderBy(F.col("status_rank").desc(), F.col("event_ts").desc())
    latest = events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    firsts = events.groupBy("order_id").agg(
        F.min(F.when(F.col("status") == "PLACED", F.col("event_ts"))).alias("placed_ts"),
        F.first("amount", ignorenulls=True).alias("any_amount"),
        F.first("payment_method", ignorenulls=True).alias("any_payment_method"),
    )
    return latest.join(firsts, "order_id").select(
        "order_id",
        "customer_id",
        "session_id",
        "restaurant_id",
        "zone",
        "status",
        "status_rank",
        F.coalesce("amount", "any_amount").alias("amount"),
        F.coalesce("payment_method", "any_payment_method").alias("payment_method"),
        "placed_ts",
        F.col("event_ts").alias("last_event_ts"),
    )


def upsert_orders_current(spark: SparkSession, events: DataFrame, path: str) -> None:
    """Out-of-order-safe upsert: an order's status only ever moves forward.

    A late PICKED_UP arriving after DELIVERED must not regress the order, so
    we update the status only when the incoming rank is higher (or equal with
    a later event time). Re-applying the same batch yields the same result,
    which is what makes the foreachBatch retry-safe."""
    src = latest_per_order(events)
    newer = (
        "(s.status_rank > t.status_rank) OR "
        "(s.status_rank = t.status_rank AND s.last_event_ts > t.last_event_ts)"
    )
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(src.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(set={
            "status": f"CASE WHEN {newer} THEN s.status ELSE t.status END",
            "status_rank": f"CASE WHEN {newer} THEN s.status_rank ELSE t.status_rank END",
            "last_event_ts": "GREATEST(t.last_event_ts, s.last_event_ts)",
            "placed_ts": "LEAST(t.placed_ts, s.placed_ts)",
            "amount": "COALESCE(t.amount, s.amount)",
            "payment_method": "COALESCE(t.payment_method, s.payment_method)",
            "session_id": "COALESCE(t.session_id, s.session_id)",
            "updated_at": "current_timestamp()",
        })
        .whenNotMatchedInsert(values={
            "order_id": "s.order_id",
            "customer_id": "s.customer_id",
            "session_id": "s.session_id",
            "restaurant_id": "s.restaurant_id",
            "zone": "s.zone",
            "status": "s.status",
            "status_rank": "s.status_rank",
            "amount": "s.amount",
            "payment_method": "s.payment_method",
            "placed_ts": "s.placed_ts",
            "last_event_ts": "s.last_event_ts",
            "updated_at": "current_timestamp()",
        })
        .execute()
    )


def _retry_on_conflict(fn, attempts: int = 3, backoff_s: float = 2.0) -> None:
    """Retry a Delta write that lost an optimistic-concurrency race (e.g. with
    an OPTIMIZE running at the same time). Safe only because every write in
    this module is idempotent."""
    for attempt in range(1, attempts + 1):
        try:
            fn()
            return
        except Exception as exc:  # delta raises Py4J-wrapped Concurrent*Exception
            if "Concurrent" not in type(exc).__name__ + str(exc)[:300] or attempt == attempts:
                raise
            log.warning("Delta write conflict (attempt %d/%d), retrying: %s", attempt, attempts,
                        str(exc).splitlines()[0])
            time.sleep(backoff_s * attempt)


def process_order_batch(
    spark: SparkSession,
    batch: DataFrame,
    batch_id: int,
    *,
    paths: Paths,
    resolver: SchemaResolver,
    max_future_skew_seconds: int,
    app_id: str = "silver_orders",
) -> dict:
    """foreachBatch body for order events. Every write is idempotent, so if
    the batch is retried after a crash half-way through, the result is the
    same as if it had run once."""
    checked = decode_and_validate_orders(batch, resolver, max_future_skew_seconds).persist()
    try:
        invalid = to_dead_letter(checked.filter(~F.col("is_valid")), "silver_orders")
        (
            invalid.write.format("delta").mode("append").partitionBy("detected_date")
            # Delta idempotent-write: (txnAppId, txnVersion) already committed
            # => this write is skipped on retry.
            .option("txnAppId", f"{app_id}_dlq")
            .option("txnVersion", batch_id)
            .save(paths.dead_letter)
        )

        events = to_silver_order_events(checked.filter(F.col("is_valid"))).persist()
        try:
            _retry_on_conflict(lambda: merge_order_events(spark, events, paths.silver_order_events))
            _retry_on_conflict(lambda: upsert_orders_current(spark, events, paths.silver_orders_current))
            counts = {"valid": events.count()}
        finally:
            events.unpersist()
        counts["invalid"] = checked.filter(~F.col("is_valid")).count()
        log.info("silver_orders batch %s: %s", batch_id, counts)
        return counts
    finally:
        checked.unpersist()


# ------------------------------------------------------------------ clickstream
def parse_and_validate_clicks(bronze: DataFrame, max_future_skew_seconds: int) -> DataFrame:
    """Works on streaming or static DataFrames."""
    parsed = bronze.withColumn(
        "_evt", F.from_json(F.col("value").cast("string"), CLICK_EVENT_JSON_SCHEMA)
    )
    # A syntactically broken document yields NULL (or all-NULL fields).
    unparseable = F.col("_evt").isNull() | (
        F.col("_evt.event_id").isNull()
        & F.col("_evt.session_id").isNull()
        & F.col("_evt.event_ts").isNull()
    )
    flat = parsed.select(
        *[F.col(c) for c in bronze.columns],
        *[F.col(f"_evt.{f.name}").alias(f.name) for f in CLICK_EVENT_JSON_SCHEMA.fields if f.name != "event_ts"],
        F.timestamp_millis(F.col("_evt.event_ts")).alias("event_ts"),
        F.when(unparseable, F.lit("MALFORMED_JSON")).alias("parse_error"),
    )
    return apply_rules(flat, click_rules(max_future_skew_seconds), "parse_error")


def to_silver_clicks(valid: DataFrame) -> DataFrame:
    return (
        valid.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(*[F.col(f.name) for f in SILVER_CLICKSTREAM.fields])
    )
