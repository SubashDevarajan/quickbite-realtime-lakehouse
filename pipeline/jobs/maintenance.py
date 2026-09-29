"""Table maintenance: compaction, Z-ordering, and vacuum.

Streaming writes produce many small files (one set per micro-batch). Small
files make every read slower, so on a schedule we:

* OPTIMIZE   - bin-pack small files into ~1 GB ones (safe to run while the
               streams are writing; it only rewrites data, dataChange=false,
               so downstream Delta streams do not re-read it),
* ZORDER BY  - co-locate rows on the columns queries filter by, so data
               skipping can prune files,
* VACUUM     - delete files no longer referenced by the table, keeping the
               default 7 days so time travel and slow readers still work.

Run:  python -m pipeline.jobs.maintenance            (all tables)
      python -m pipeline.jobs.maintenance --no-vacuum
"""

from __future__ import annotations

import argparse
import logging

from delta.tables import DeltaTable

from pipeline.config import SETTINGS
from pipeline.spark_session import build_spark

log = logging.getLogger("pipeline.maintenance")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-vacuum", action="store_true")
    args = parser.parse_args()

    spark = build_spark("maintenance")
    p = SETTINGS.paths

    plan = [
        # (path, z-order columns or None)
        (p.bronze_events, None),
        (p.silver_order_events, ["zone", "order_id"]),
        (p.silver_orders_current, ["order_id"]),
        (p.silver_clickstream, ["customer_id"]),
        (p.dead_letter, None),
        (p.gold_zone_metrics, None),
        (p.gold_sla_alerts, None),
        (p.gold_checkout_conversion, None),
        (p.gold_sessions, None),
    ]

    for path, zorder in plan:
        if not DeltaTable.isDeltaTable(spark, path):
            log.info("skip (not created yet): %s", path)
            continue
        table = DeltaTable.forPath(spark, path)
        before = table.detail().select("numFiles").first()[0]
        optimizer = table.optimize()
        result = optimizer.executeZOrderBy(*zorder) if zorder else optimizer.executeCompaction()
        metrics = result.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        log.info("OPTIMIZE %s: files %s -> %s (added %s, removed %s)%s", path, before,
                 table.detail().select("numFiles").first()[0], metrics[0], metrics[1],
                 f" ZORDER BY {zorder}" if zorder else "")
        if not args.no_vacuum:
            table.vacuum()  # default retention (7 days)

    spark.stop()


if __name__ == "__main__":
    main()
