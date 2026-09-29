"""Bronze: Kafka -> Delta, byte-for-byte.

Bronze does no parsing and no filtering. It lands every Kafka record with its
coordinates (topic, partition, offset, broker timestamp) so that:

* silver can be rebuilt from bronze at any time (Kafka retention is finite),
* a bad deploy of silver never loses data,
* every silver row can be traced back to an exact Kafka offset.

Exactly-once: Spark stores the Kafka offsets of each micro-batch in the
checkpoint *and* the Delta sink records the batch id in the table's
transaction log. On restart the batch is either fully committed or replayed
and skipped as already written, never duplicated or lost.

Run:  python -m pipeline.jobs.bronze_ingest
"""

from __future__ import annotations

from pyspark.sql import functions as F

from pipeline.jobs.common import run_until_terminated, start
from pipeline.tables import ensure_bronze


def main() -> None:
    spark, s = start("bronze_ingest")
    paths = s.paths
    ensure_bronze(spark, paths)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", f"{s.order_topic},{s.click_topic}")
        .option("startingOffsets", s.starting_offsets)
        # Back-pressure: cap how much one micro-batch may pull, so a backlog
        # (e.g. after downtime) is drained in bounded, predictable batches.
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)
        # Topic retention may delete offsets we have not read yet during long
        # outages. Log and continue instead of halting the platform; the gap
        # is visible in Kafka UI / consumer metrics.
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = raw.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key"),
        F.col("value"),
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingest_date"),
    )

    (
        bronze.writeStream.format("delta")
        .queryName("bronze_ingest")
        .outputMode("append")
        .partitionBy("topic", "ingest_date")
        .option("checkpointLocation", paths.checkpoint("bronze_ingest"))
        .trigger(processingTime=s.trigger_interval)
        .start(paths.bronze_events)
    )
    run_until_terminated(spark)


if __name__ == "__main__":
    main()
