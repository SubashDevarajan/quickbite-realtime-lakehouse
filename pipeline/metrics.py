"""Streaming observability.

A ``StreamingQueryListener`` records every micro-batch's progress (rows in,
rows/sec, batch duration, watermark, state size, rows dropped as too late)
to a JSON-lines file per query. The dashboard reads these files, and they
are where the throughput / latency numbers in the README come from.

In production these would go to Prometheus / Datadog / Azure Monitor; the
listener is the same, only the sink changes.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("pipeline.metrics")


def summarize_progress(progress: dict) -> dict:
    """Flatten Spark's progress JSON into the fields we chart and alert on."""
    duration = progress.get("durationMs") or {}
    event_time = progress.get("eventTime") or {}
    state_ops = progress.get("stateOperators") or []
    return {
        "query": progress.get("name"),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "num_input_rows": progress.get("numInputRows", 0),
        "input_rows_per_sec": round(progress.get("inputRowsPerSecond") or 0.0, 2),
        "processed_rows_per_sec": round(progress.get("processedRowsPerSecond") or 0.0, 2),
        "batch_duration_ms": duration.get("triggerExecution"),
        "add_batch_ms": duration.get("addBatch"),
        "watermark": event_time.get("watermark"),
        "state_rows": sum(op.get("numRowsTotal", 0) for op in state_ops),
        "state_memory_bytes": sum(op.get("memoryUsedBytes", 0) for op in state_ops),
        "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in state_ops),
    }


class JsonlProgressListener(StreamingQueryListener):
    def __init__(self, out_dir: str):
        super().__init__()
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event):
        log.info("query started: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event):
        try:
            summary = summarize_progress(json.loads(event.progress.json))
            name = summary["query"] or "unnamed"
            with open(self.out_dir / f"{name}.jsonl", "a") as fh:
                fh.write(json.dumps(summary) + "\n")
            if summary["num_input_rows"]:
                log.info(
                    "[%s] batch=%s rows=%s in/s=%.0f proc/s=%.0f dur=%sms dropped_late=%s",
                    name, summary["batch_id"], summary["num_input_rows"],
                    summary["input_rows_per_sec"], summary["processed_rows_per_sec"],
                    summary["batch_duration_ms"], summary["rows_dropped_by_watermark"],
                )
        except Exception:  # never let observability kill the pipeline
            log.exception("failed to record progress")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query terminated: %s exception=%s", event.id, event.exception)


def register(spark: SparkSession, out_dir: str) -> None:
    if os.getenv("DISABLE_METRICS_LISTENER") == "1":
        return
    spark.streams.addListener(JsonlProgressListener(out_dir))
