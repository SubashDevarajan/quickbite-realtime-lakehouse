"""Prove the pipeline's correctness claims against ground truth.

The generator writes a manifest when it stops: how many *unique, valid*
events it produced per topic, how many duplicates and malformed records it
injected. This script waits until the lakehouse stops changing (pipeline
fully drained) and then checks:

1. silver has no duplicate event_ids                     (de-dup works)
2. silver row count == unique valid events produced       (nothing lost, nothing extra)
3. dead-letter count == malformed records produced         (every bad record caught)
4. orders_current never regressed an order's status       (out-of-order handling works)

Run it after a chaos experiment (killing a job mid-batch, replaying silver
from bronze, ...) to show the result is still exactly-once.

Usage (stack running):
    docker compose stop generator          # writes the manifest
    docker compose run --rm tools python -m scripts.verify_exactly_once
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from pipeline.config import SETTINGS
from pipeline.spark_session import build_spark


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=os.getenv("MANIFEST_PATH", f"{SETTINGS.lake_root}/generator/manifest.json"))
    ap.add_argument("--poll-seconds", type=int, default=15)
    ap.add_argument("--stable-polls", type=int, default=3)
    ap.add_argument("--timeout-seconds", type=int, default=600)
    args = ap.parse_args()

    spark = build_spark("verify", shuffle_partitions=8)
    p = SETTINGS.paths
    read = lambda path: spark.read.format("delta").load(path)  # noqa: E731

    # ---------------------------------------------------- wait until drained
    print("waiting for the pipeline to drain (counts unchanged for "
          f"{args.stable_polls} x {args.poll_seconds}s)...")
    started, last, stable = time.time(), None, 0
    while True:
        snapshot = (
            read(p.bronze_events).count(),
            read(p.silver_order_events).count(),
            read(p.silver_clickstream).count(),
            read(p.dead_letter).count(),
        )
        stable = stable + 1 if snapshot == last else 0
        last = snapshot
        print(f"  bronze={snapshot[0]} silver_orders={snapshot[1]} silver_clicks={snapshot[2]} dlq={snapshot[3]}")
        if stable >= args.stable_polls:
            break
        if time.time() - started > args.timeout_seconds:
            print("timed out waiting for a stable state; checking anyway")
            break
        time.sleep(args.poll_seconds)

    manifest = None
    if Path(args.manifest).exists():
        manifest = json.loads(Path(args.manifest).read_text())
    else:
        print(f"\n!! no manifest at {args.manifest}. Stop the generator first "
              "(docker compose stop generator) for the ground-truth checks.\n")

    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        results.append((name, ok, detail))

    orders, clicks, dlq = read(p.silver_order_events), read(p.silver_clickstream), read(p.dead_letter)

    n_orders, n_orders_distinct = orders.count(), orders.select("event_id").distinct().count()
    check("no duplicate order events in silver", n_orders == n_orders_distinct,
          f"rows={n_orders} distinct_event_ids={n_orders_distinct}")

    n_clicks, n_clicks_distinct = clicks.count(), clicks.select("event_id").distinct().count()
    check("no duplicate click events in silver", n_clicks == n_clicks_distinct,
          f"rows={n_clicks} distinct_event_ids={n_clicks_distinct}")

    current = read(p.silver_orders_current)
    max_rank = orders.groupBy("order_id").agg(F.max("status_rank").alias("max_rank"))
    regressed = current.join(max_rank, "order_id").filter(F.col("status_rank") < F.col("max_rank")).count()
    check("orders_current never regressed a status", regressed == 0, f"regressed_orders={regressed}")

    dlq_by_topic = {r["source_topic"]: r["count"] for r in dlq.groupBy("source_topic").count().collect()}

    if manifest:
        expected = manifest["unique_valid_events"]
        sent = manifest["sent"]
        check("silver order events == unique valid produced",
              n_orders == expected.get("order_events", 0),
              f"silver={n_orders} produced={expected.get('order_events', 0)}")
        check("silver click events == unique valid produced",
              n_clicks == expected.get("clickstream", 0),
              f"silver={n_clicks} produced={expected.get('clickstream', 0)}")
        for topic in ("order_events", "clickstream"):
            produced_bad = sent.get(f"{topic}:malformed", 0)
            check(f"every malformed {topic} record is in the DLQ",
                  dlq_by_topic.get(topic, 0) == produced_bad,
                  f"dlq={dlq_by_topic.get(topic, 0)} malformed_produced={produced_bad}")

    print("\nDLQ breakdown:")
    for r in dlq.groupBy("source_topic", "primary_reason").count().orderBy("source_topic", "primary_reason").collect():
        print(f"  {r['source_topic']:14s} {r['primary_reason']:26s} {r['count']}")

    print("\nRESULTS")
    for name, ok, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:48s} {detail}")
    spark.stop()
    return 0 if all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
