"""Spark-side schemas and domain constants."""

from __future__ import annotations

from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_STATUSES = ("PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED")

# Lifecycle order. Terminal states share the highest rank; when two events
# have the same rank the one with the later event_ts wins.
STATUS_RANK = {"PLACED": 1, "ACCEPTED": 2, "PICKED_UP": 3, "DELIVERED": 4, "CANCELLED": 4}

CLICK_EVENT_TYPES = ("app_open", "search", "view_restaurant", "add_to_cart", "checkout_started")

# Canonical shape of a decoded order event = the *latest* Avro schema version.
# Older versions are aligned to this (missing fields become NULL).
ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("session_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone", StringType()),
    StructField("status", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("event_ts", TimestampType()),
    StructField("payment_method", StringType()),
])

# Clickstream arrives as JSON. event_ts is epoch millis.
CLICK_EVENT_JSON_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone", StringType()),
    StructField("device", StringType()),
    StructField("event_ts", LongType()),
])
