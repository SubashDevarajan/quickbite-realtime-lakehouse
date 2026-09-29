"""Gold: business-facing streaming aggregates.

Four independent streaming queries (own checkpoint each, so one can be reset
without touching the others) reading silver Delta tables as streams.

Run:  python -m pipeline.jobs.gold
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from pipeline import gold
from pipeline.jobs.common import run_until_terminated, start
from pipeline.tables import ensure_silver


def main() -> None:
    spark, s = start("gold")
    paths = s.paths
    ensure_silver(spark, paths)  # so gold can start before silver has written

    def stream(path: str) -> DataFrame:
        return spark.readStream.format("delta").option("maxFilesPerTrigger", 200).load(path)

    def sink(df: DataFrame, name: str, path: str) -> None:
        (
            df.writeStream.format("delta")
            .queryName(name)
            .outputMode("append")
            .option("checkpointLocation", paths.checkpoint(name))
            .trigger(processingTime=s.trigger_interval)
            .start(path)
        )

    wm = s.watermark_delay

    sink(gold.zone_metrics_1m(stream(paths.silver_order_events), wm),
         "gold_zone_metrics_1m", paths.gold_zone_metrics)

    sink(gold.sla_alerts(stream(paths.silver_order_events), s.alert_watermark_delay,
                         s.order_sla_seconds, s.order_state_ttl_seconds),
         "gold_sla_alerts", paths.gold_sla_alerts)

    sink(gold.checkout_conversion(stream(paths.silver_clickstream), stream(paths.silver_order_events),
                                  s.alert_watermark_delay, s.checkout_to_order_seconds),
         "gold_checkout_conversion", paths.gold_checkout_conversion)

    sink(gold.user_sessions(stream(paths.silver_clickstream), wm, s.session_gap_seconds),
         "gold_user_sessions", paths.gold_sessions)

    run_until_terminated(spark)


if __name__ == "__main__":
    main()
