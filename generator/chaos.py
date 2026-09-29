"""Chaos injection: makes the synthetic stream look like production traffic.

Real event streams are never clean. This module wraps the simulator output and
injects the failure modes the pipeline is designed to survive:

* **duplicates**  - the same event (same ``event_id``) sent twice, as happens
  with client retries or at-least-once upstream services.
* **late / out-of-order events** - an event is held back and released
  30s-4min later with its *original* event time. Some land inside the
  streaming watermark (handled), some beyond it (dropped by windowed
  aggregations and counted in metrics).
* **malformed events** - extra bad records: bytes that are not Avro at all,
  unknown statuses, negative amounts, timestamps in the future, empty
  required fields, and broken JSON. These must end up in the dead-letter
  queue, never in silver.
"""

from __future__ import annotations

import heapq
import itertools
import json
import random
from dataclasses import dataclass, replace
from decimal import Decimal

from generator.simulator import CLICK_TOPIC, ORDER_TOPIC, Emission

ORDER_MALFORMATIONS = (
    "garbage_bytes",
    "unknown_status",
    "negative_amount",
    "future_timestamp",
    "empty_zone",
)
CLICK_MALFORMATIONS = ("broken_json", "missing_event_id")


@dataclass
class ChaosConfig:
    duplicate_rate: float = 0.03
    late_rate: float = 0.03
    late_delay_s: tuple[float, float] = (30.0, 240.0)
    malformed_rate: float = 0.01
    enabled: bool = True


class ChaosInjector:
    def __init__(self, config: ChaosConfig, rng: random.Random):
        self.cfg = config
        self.rng = rng
        self._held: list[tuple[int, int, Emission]] = []
        self._seq = itertools.count()

    @property
    def held(self) -> int:
        return len(self._held)

    def apply(self, emissions: list[Emission], now_ms: int) -> list[Emission]:
        """Return what should be sent *now*: chaos-applied input plus any
        previously held late events whose release time has come."""
        out: list[Emission] = []
        for em in emissions:
            if not self.cfg.enabled:
                out.append(em)
                continue

            if self.rng.random() < self.cfg.late_rate:
                release = now_ms + int(self.rng.uniform(*self.cfg.late_delay_s) * 1000)
                heapq.heappush(self._held, (release, next(self._seq), replace(em, tag="late")))
            else:
                out.append(em)

            if self.rng.random() < self.cfg.duplicate_rate:
                out.append(replace(em, tag="duplicate", expected_in_silver=False))

            if self.rng.random() < self.cfg.malformed_rate:
                out.append(self._malformed_from(em, now_ms))

        out.extend(self.release(now_ms))
        return out

    def release(self, now_ms: int) -> list[Emission]:
        out = []
        while self._held and self._held[0][0] <= now_ms:
            out.append(heapq.heappop(self._held)[2])
        return out

    def flush(self) -> list[Emission]:
        """Release everything still held (used on shutdown so no event is lost)."""
        out = [item[2] for item in sorted(self._held)]
        self._held.clear()
        return out

    # ------------------------------------------------------------------
    def _malformed_from(self, em: Emission, now_ms: int) -> Emission:
        new_id = f"E-bad{self.rng.getrandbits(48):012x}"
        if em.topic == ORDER_TOPIC:
            reason = self.rng.choice(ORDER_MALFORMATIONS)
            if reason == "garbage_bytes":
                # No Confluent magic byte, not Avro: undecodable.
                value: object = bytes(self.rng.getrandbits(8) for _ in range(24))
                return Emission(ORDER_TOPIC, em.key, value, "raw", f"malformed:{reason}", False)
            value = dict(em.value, event_id=new_id)
            if reason == "unknown_status":
                value["status"] = "TELEPORTED"
            elif reason == "negative_amount":
                value["amount"] = Decimal("-1.00")
            elif reason == "future_timestamp":
                value["event_ts"] = now_ms + 24 * 3600 * 1000
            elif reason == "empty_zone":
                value["zone"] = ""
            return Emission(ORDER_TOPIC, em.key, value, "order", f"malformed:{reason}", False)

        reason = self.rng.choice(CLICK_MALFORMATIONS)
        if reason == "broken_json":
            raw = json.dumps(dict(em.value, event_id=new_id))[:-7].encode()
            return Emission(CLICK_TOPIC, em.key, raw, "raw", f"malformed:{reason}", False)
        value = {k: v for k, v in em.value.items() if k != "event_id"}
        return Emission(CLICK_TOPIC, em.key, value, "click", f"malformed:{reason}", False)
