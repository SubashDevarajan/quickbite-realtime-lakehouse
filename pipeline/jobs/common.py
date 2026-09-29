"""Shared bootstrapping for the streaming jobs."""

from __future__ import annotations

import logging
import os
import signal

from pyspark.sql import SparkSession

from pipeline import metrics
from pipeline.config import SETTINGS, Settings
from pipeline.spark_session import build_spark


def start(app_name: str) -> tuple[SparkSession, Settings]:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    spark = build_spark(app_name)
    metrics.register(spark, os.getenv("METRICS_DIR", SETTINGS.paths.metrics_dir))
    return spark, SETTINGS


def run_until_terminated(spark: SparkSession) -> None:
    """Block until any query fails (then exit non-zero so the container
    restarts and resumes from its checkpoint) or the process is stopped."""
    log = logging.getLogger("pipeline")

    def _graceful(*_):
        log.info("stop requested; stopping %d queries", len(spark.streams.active))
        for q in spark.streams.active:
            q.stop()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        log.exception("a streaming query failed; exiting so the supervisor restarts us")
        for q in spark.streams.active:
            q.stop()
        raise SystemExit(1) from None
    for q in spark.streams.active:
        q.stop()
