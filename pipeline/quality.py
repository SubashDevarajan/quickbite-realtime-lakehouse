"""Data-quality rules expressed as Spark Column expressions.

Each rule is ``(reason_code, is_bad_expression)``. A record gets an array of
every rule it violates, so the dead-letter queue says *all* the reasons a
record was rejected, not just the first. Expressions are wrapped in
``coalesce(..., True)`` where NULL means "cannot prove it is valid".

Rules live here, in one place, so they are reviewable and unit-tested
independently of the streaming jobs.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline.schemas import CLICK_EVENT_TYPES, ORDER_STATUSES


def _blank(col: str) -> Column:
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


def _bad(expr: Column) -> Column:
    return F.coalesce(expr, F.lit(True))


def order_rules(max_future_skew_seconds: int) -> list[tuple[str, Column]]:
    return [
        ("MISSING_EVENT_ID", _blank("event_id")),
        ("MISSING_ORDER_ID", _blank("order_id")),
        ("MISSING_CUSTOMER_ID", _blank("customer_id")),
        ("MISSING_ZONE", _blank("zone")),
        ("UNKNOWN_STATUS", _bad(~F.col("status").isin(*ORDER_STATUSES))),
        ("NEGATIVE_AMOUNT", F.coalesce(F.col("amount") < 0, F.lit(False))),
        (
            "MISSING_AMOUNT_ON_PLACED",
            F.coalesce((F.col("status") == "PLACED") & F.col("amount").isNull(), F.lit(False)),
        ),
        ("MISSING_EVENT_TS", F.col("event_ts").isNull()),
        (
            "FUTURE_EVENT_TS",
            # Compare with the broker timestamp, not the processing clock, so
            # re-processing old data gives the same verdict (determinism).
            F.coalesce(
                F.col("event_ts")
                > F.col("kafka_ts") + F.expr(f"INTERVAL {max_future_skew_seconds} SECONDS"),
                F.lit(False),
            ),
        ),
    ]


def click_rules(max_future_skew_seconds: int) -> list[tuple[str, Column]]:
    return [
        ("MISSING_EVENT_ID", _blank("event_id")),
        ("MISSING_SESSION_ID", _blank("session_id")),
        ("MISSING_CUSTOMER_ID", _blank("customer_id")),
        ("UNKNOWN_EVENT_TYPE", _bad(~F.col("event_type").isin(*CLICK_EVENT_TYPES))),
        ("MISSING_EVENT_TS", F.col("event_ts").isNull()),
        (
            "FUTURE_EVENT_TS",
            F.coalesce(
                F.col("event_ts")
                > F.col("kafka_ts") + F.expr(f"INTERVAL {max_future_skew_seconds} SECONDS"),
                F.lit(False),
            ),
        ),
    ]


def apply_rules(df: DataFrame, rules: list[tuple[str, Column]], parse_error_col: str) -> DataFrame:
    """Add ``dq_errors`` (array<string>) and ``is_valid`` (boolean).

    If the record could not be parsed at all, the parse error is the only
    reason reported (field-level rules would just be noise).
    """
    rule_hits = F.filter(
        F.array(*[F.when(is_bad, F.lit(reason)) for reason, is_bad in rules]),
        lambda x: x.isNotNull(),
    )
    errors = F.when(
        F.col(parse_error_col).isNotNull(), F.array(F.col(parse_error_col))
    ).otherwise(rule_hits)
    return df.withColumn("dq_errors", errors).withColumn("is_valid", F.size("dq_errors") == 0)
