"""Delete silver and/or gold tables *and* their checkpoints so they are
rebuilt from the layer below on the next start.

This is the "replay" drill: bronze keeps every raw Kafka record, so silver
and gold can always be recomputed (after a bug fix, a new DQ rule, a schema
change...). After the rebuild, ``scripts/verify_exactly_once.py`` must still
pass, which shows the pipeline is deterministic and idempotent.

Tables and checkpoints are always reset together: a checkpoint without its
table (or vice versa) would make Delta skip or duplicate batches.

Usage:  python -m scripts.reset_layers silver gold
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from pipeline.config import SETTINGS

LAYERS = {
    "silver": {
        "tables": ["silver/order_events", "silver/orders_current", "silver/clickstream", "silver/dead_letter"],
        "checkpoints": ["silver_orders", "silver_clickstream", "silver_clickstream_dlq"],
    },
    "gold": {
        "tables": ["gold/zone_metrics_1m", "gold/sla_alerts", "gold/checkout_conversion", "gold/user_sessions"],
        "checkpoints": ["gold_zone_metrics_1m", "gold_sla_alerts", "gold_checkout_conversion",
                        "gold_user_sessions"],
    },
}


def main(layers: list[str]) -> None:
    root = Path(SETTINGS.lake_root)
    if "silver" in layers and "gold" not in layers:
        print("note: resetting silver also requires resetting gold (gold streams read silver); adding gold")
        layers = [*layers, "gold"]
    for layer in layers:
        spec = LAYERS[layer]
        targets = [root / "lake" / t for t in spec["tables"]] + [root / "checkpoints" / c for c in spec["checkpoints"]]
        for target in targets:
            if target.exists():
                shutil.rmtree(target)
                print(f"deleted {target}")
        metrics_dir = root / "metrics"
        for f in metrics_dir.glob(f"{layer}_*.jsonl"):
            f.unlink()


if __name__ == "__main__":
    requested = sys.argv[1:] or ["silver", "gold"]
    unknown = set(requested) - set(LAYERS)
    if unknown:
        sys.exit(f"unknown layer(s): {unknown}; choose from {list(LAYERS)}")
    main(requested)
