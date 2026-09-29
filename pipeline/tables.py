"""Explicit Delta table definitions.

Tables are created up front with explicit schemas instead of being inferred
from the first micro-batch. That makes the contract reviewable in one file,
lets MERGE targets exist before the first batch, and stops an accidental
schema change upstream from silently widening a table.
"""

from __future__ import annotations

import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.config import Paths

SILVER_ORDER_EVENTS = StructType([
    StructField("event_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType()),
    StructField("session_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone", StringType()),
    StructField("status", StringType()),
    StructField("status_rank", IntegerType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("payment_method", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("event_date", DateType()),
    StructField("schema_id", IntegerType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("bronze_ingested_at", TimestampType()),
    StructField("silver_processed_at", TimestampType()),
])

SILVER_ORDERS_CURRENT = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType()),
    StructField("session_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone", StringType()),
    StructField("status", StringType()),
    StructField("status_rank", IntegerType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("payment_method", StringType()),
    StructField("placed_ts", TimestampType()),
    StructField("last_event_ts", TimestampType()),
    StructField("updated_at", TimestampType()),
])

SILVER_CLICKSTREAM = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone", StringType()),
    StructField("device", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("event_date", DateType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("silver_processed_at", TimestampType()),
])

DEAD_LETTER = StructType([
    StructField("source_topic", StringType()),
    StructField("pipeline_stage", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("event_id", StringType()),
    StructField("primary_reason", StringType()),
    StructField("error_reasons", ArrayType(StringType())),
    StructField("raw_value", BinaryType()),
    StructField("detected_at", TimestampType()),
    StructField("detected_date", DateType()),
])

BRONZE_EVENTS = StructType([
    StructField("topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("ingested_at", TimestampType()),
    StructField("ingest_date", DateType()),
])


def _create(spark: SparkSession, path: str, schema: StructType, partition_by: list[str] | None = None,
            comment: str = "", attempts: int = 5) -> None:
    # Several jobs start at once and may race to create the same table; the
    # loser of the race gets a commit conflict, waits, and then finds the
    # table already there.
    for attempt in range(1, attempts + 1):
        try:
            builder = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).comment(comment)
            if partition_by:
                builder = builder.partitionedBy(*partition_by)
            builder.execute()
            return
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)


def ensure_bronze(spark: SparkSession, paths: Paths) -> None:
    _create(spark, paths.bronze_events, BRONZE_EVENTS, ["topic", "ingest_date"],
            "Raw Kafka records, byte-for-byte. Replay source for silver.")


def ensure_silver(spark: SparkSession, paths: Paths) -> None:
    _create(spark, paths.silver_order_events, SILVER_ORDER_EVENTS, ["event_date"],
            "Validated, de-duplicated order events (one row per event_id).")
    _create(spark, paths.silver_orders_current, SILVER_ORDERS_CURRENT, None,
            "Latest known state per order (out-of-order safe upsert).")
    _create(spark, paths.silver_clickstream, SILVER_CLICKSTREAM, ["event_date"],
            "Validated, de-duplicated clickstream events.")
    _create(spark, paths.dead_letter, DEAD_LETTER, ["detected_date"],
            "Records rejected by decoding or data-quality rules, with reasons and raw bytes.")
