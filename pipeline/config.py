"""Central configuration. Everything is overridable through environment
variables so the same code runs on a laptop (local paths), in Docker, or on a
cloud cluster (set LAKE_ROOT to an abfss://, gs:// or s3a:// URI)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Paths:
    root: str

    def table(self, layer: str, name: str) -> str:
        return f"{self.root}/lake/{layer}/{name}"

    def checkpoint(self, name: str) -> str:
        return f"{self.root}/checkpoints/{name}"

    @property
    def metrics_dir(self) -> str:
        return f"{self.root}/metrics"

    # Bronze
    @property
    def bronze_events(self) -> str:
        return self.table("bronze", "events_raw")

    # Silver
    @property
    def silver_order_events(self) -> str:
        return self.table("silver", "order_events")

    @property
    def silver_orders_current(self) -> str:
        return self.table("silver", "orders_current")

    @property
    def silver_clickstream(self) -> str:
        return self.table("silver", "clickstream")

    @property
    def dead_letter(self) -> str:
        return self.table("silver", "dead_letter")

    # Gold
    @property
    def gold_zone_metrics(self) -> str:
        return self.table("gold", "zone_metrics_1m")

    @property
    def gold_sla_alerts(self) -> str:
        return self.table("gold", "sla_alerts")

    @property
    def gold_checkout_conversion(self) -> str:
        return self.table("gold", "checkout_conversion")

    @property
    def gold_sessions(self) -> str:
        return self.table("gold", "user_sessions")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:29092")
    schema_registry_url: str = _env("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    order_topic: str = _env("ORDER_TOPIC", "order_events")
    click_topic: str = _env("CLICK_TOPIC", "clickstream")
    lake_root: str = _env("LAKE_ROOT", "data")

    trigger_interval: str = _env("TRIGGER_INTERVAL", "10 seconds")
    max_offsets_per_trigger: int = int(_env("MAX_OFFSETS_PER_TRIGGER", "50000"))
    starting_offsets: str = _env("STARTING_OFFSETS", "earliest")

    # Event-time tolerances. The watermark is how late an event may arrive
    # and still be counted in a window; anything later is dropped from the
    # windowed aggregates (it is still kept in silver).
    watermark_delay: str = _env("WATERMARK_DELAY", "2 minutes")
    # Longer watermark for queries where a dropped late event causes a
    # *wrong answer* rather than a slightly low count: a late DELIVERED
    # dropped by the SLA tracker would raise a false breach, and a late
    # PLACED dropped by the conversion join would mark a checkout abandoned.
    # Must exceed the worst expected lateness (generator: 4 minutes).
    alert_watermark_delay: str = _env("ALERT_WATERMARK_DELAY", "5 minutes")
    # Order SLA (compressed for the demo; think "45 minutes" in production).
    order_sla_seconds: int = int(_env("ORDER_SLA_SECONDS", "240"))
    # How long per-order state is kept after an SLA breach or a terminal
    # status, to absorb stragglers without re-alerting. Bounds state size.
    order_state_ttl_seconds: int = int(_env("ORDER_STATE_TTL_SECONDS", "900"))
    session_gap_seconds: int = int(_env("SESSION_GAP_SECONDS", "180"))
    checkout_to_order_seconds: int = int(_env("CHECKOUT_TO_ORDER_SECONDS", "300"))
    clickstream_dedup_window: str = _env("CLICKSTREAM_DEDUP_WINDOW", "10 minutes")
    max_future_skew_seconds: int = int(_env("MAX_FUTURE_SKEW_SECONDS", "300"))

    @property
    def paths(self) -> Paths:
        return Paths(self.lake_root.rstrip("/"))


SETTINGS = Settings()
