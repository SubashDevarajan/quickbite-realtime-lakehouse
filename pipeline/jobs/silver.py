"""Silver: decode, validate, de-duplicate, and route bad records to a DLQ.

Three streaming queries, all reading the bronze Delta table:

1. ``silver_orders``        - Avro order events via ``foreachBatch``:
   per-schema-id Avro decoding, data-quality rules, DLQ, insert-only MERGE
   on event_id (cross-batch de-dup), out-of-order-safe upsert of the
   current order state.
2. ``silver_clickstream``   - JSON clickstream, de-duplicated natively with
   ``dropDuplicatesWithinWatermark`` (Spark 3.5) and appended.
3. ``silver_clickstream_dlq`` - clickstream records that failed parsing or DQ.

Two different de-dup strategies on purpose; see docs/design-decisions.md.

Run:  python -m pipeline.jobs.silver
"""

from __future__ import annotations

from pyspark.sql import functions as F

from pipeline.avro_decode import RegistrySchemaResolver
from pipeline.jobs.common import run_until_terminated, start
from pipeline.silver import (
    parse_and_validate_clicks,
    process_order_batch,
    to_dead_letter,
    to_silver_clicks,
)
from pipeline.tables import ensure_bronze, ensure_silver


def main() -> None:
    spark, s = start("silver")
    paths = s.paths
    ensure_bronze(spark, paths)
    ensure_silver(spark, paths)

    def bronze_stream(topic: str):
        return (
            spark.readStream.format("delta")
            .option("maxFilesPerTrigger", 200)
            .load(paths.bronze_events)
            .where(F.col("topic") == topic)
        )

    # 1) Orders --------------------------------------------------------------
    resolver = RegistrySchemaResolver(s.schema_registry_url)

    def orders_batch(batch_df, batch_id):
        process_order_batch(
            spark, batch_df, batch_id,
            paths=paths, resolver=resolver,
            max_future_skew_seconds=s.max_future_skew_seconds,
        )

    (
        bronze_stream(s.order_topic).writeStream.queryName("silver_orders")
        .foreachBatch(orders_batch)
        .option("checkpointLocation", paths.checkpoint("silver_orders"))
        .trigger(processingTime=s.trigger_interval)
        .start()
    )

    # 2) Clickstream ---------------------------------------------------------
    clicks = parse_and_validate_clicks(bronze_stream(s.click_topic), s.max_future_skew_seconds)

    valid_clicks = (
        clicks.filter(F.col("is_valid"))
        # Duplicates of the same event arrive within minutes of each other, so
        # state only needs to remember ids for the dedup window, not forever.
        .withWatermark("event_ts", s.clickstream_dedup_window)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    (
        to_silver_clicks(valid_clicks).writeStream.format("delta")
        .queryName("silver_clickstream")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", paths.checkpoint("silver_clickstream"))
        .trigger(processingTime=s.trigger_interval)
        .start(paths.silver_clickstream)
    )

    (
        to_dead_letter(clicks.filter(~F.col("is_valid")), "silver_clickstream")
        .writeStream.format("delta")
        .queryName("silver_clickstream_dlq")
        .outputMode("append")
        .partitionBy("detected_date")
        .option("checkpointLocation", paths.checkpoint("silver_clickstream_dlq"))
        .trigger(processingTime=s.trigger_interval)
        .start(paths.dead_letter)
    )

    run_until_terminated(spark)


if __name__ == "__main__":
    main()
