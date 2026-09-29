"""Per-order SLA tracking with arbitrary stateful processing.

Business rule: an order that is not DELIVERED or CANCELLED within
``sla_ms`` of being placed is a breach, and ops should hear about it *while
it is happening*, not in tomorrow's report.

This cannot be expressed as a window aggregation: the alert must fire when
something *does not* happen. So each order keeps a small state record and an
**event-time timer** (Spark ``GroupStateTimeout.EventTimeTimeout``). When the
watermark passes ``placed + SLA`` and the order is still open, the timer fires
and we emit ``SLA_BREACH``. If the order is later delivered we emit
``DELIVERED_AFTER_BREACH`` so the alert can be closed.

The logic below is plain Python (no Spark import) so it is unit-testable in
milliseconds; ``spark_state_func`` adapts it to ``applyInPandasWithState``.

State lifecycle::

    first event --> OPEN --(timer: watermark > start+SLA)--> BREACHED --(timer: TTL)--> dropped
                     |                                           |
                     +--DELIVERED/CANCELLED--> TERMINAL <--------+
                                                  |
                                          (timer: TTL) --> dropped

Terminal orders are kept as a tombstone until the TTL so that a straggling
out-of-order event (e.g. a late PICKED_UP after DELIVERED) does not recreate
the order and trigger a false alert.
"""

from __future__ import annotations

from dataclasses import astuple, dataclass

STATUS_RANK = {"PLACED": 1, "ACCEPTED": 2, "PICKED_UP": 3, "DELIVERED": 4, "CANCELLED": 4}
TERMINAL = frozenset({"DELIVERED", "CANCELLED"})

STATE_SCHEMA = (
    "zone STRING, restaurant_id STRING, first_seen_ms LONG, placed_ms LONG, "
    "last_status STRING, last_rank INT, last_event_ms LONG, breached BOOLEAN, terminal BOOLEAN"
)
OUTPUT_SCHEMA = (
    "order_id STRING, zone STRING, restaurant_id STRING, alert_type STRING, last_status STRING, "
    "placed_ts TIMESTAMP, detected_at TIMESTAMP, open_seconds DOUBLE"
)


@dataclass
class OrderSlaState:
    zone: str
    restaurant_id: str
    first_seen_ms: int
    placed_ms: int | None
    last_status: str
    last_rank: int
    last_event_ms: int
    breached: bool = False
    terminal: bool = False

    @property
    def start_ms(self) -> int:
        # If PLACED has not arrived yet (out of order), the earliest event we
        # have seen is the best estimate of when the order started.
        return self.placed_ms if self.placed_ms is not None else self.first_seen_ms

    def to_tuple(self) -> tuple:
        return astuple(self)

    @classmethod
    def from_tuple(cls, t) -> OrderSlaState:
        return cls(*t)


@dataclass(frozen=True)
class Alert:
    order_id: str
    zone: str
    restaurant_id: str
    alert_type: str  # SLA_BREACH | DELIVERED_AFTER_BREACH
    last_status: str
    placed_ms: int
    detected_ms: int
    open_seconds: float


def _clamp_timeout(ts_ms: int, watermark_ms: int) -> int:
    # Spark rejects event-time timeouts earlier than the current watermark.
    return max(ts_ms, watermark_ms + 1)


def on_events(
    order_id: str,
    state: OrderSlaState | None,
    events: list[dict],
    *,
    sla_ms: int,
    ttl_ms: int,
    watermark_ms: int,
) -> tuple[OrderSlaState | None, list[Alert], int | None]:
    """Fold new events into the order's state.

    ``events`` items need keys: status, event_ms, zone, restaurant_id.
    Returns (new_state or None to drop, alerts, event-time timeout ms).
    """
    alerts: list[Alert] = []
    was_terminal = state.terminal if state else False

    for ev in sorted(events, key=lambda e: e["event_ms"]):
        status, ts = ev["status"], int(ev["event_ms"])
        rank = STATUS_RANK.get(status, 0)
        if state is None:
            state = OrderSlaState(
                zone=ev["zone"],
                restaurant_id=ev["restaurant_id"],
                first_seen_ms=ts,
                placed_ms=None,
                last_status=status,
                last_rank=rank,
                last_event_ms=ts,
            )
        state.first_seen_ms = min(state.first_seen_ms, ts)
        if status == "PLACED":
            state.placed_ms = ts if state.placed_ms is None else min(state.placed_ms, ts)
        # Out-of-order safe: status only moves forward in the lifecycle.
        if rank > state.last_rank or (rank == state.last_rank and ts > state.last_event_ms):
            state.last_status, state.last_rank = status, rank
        state.last_event_ms = max(state.last_event_ms, ts)

    if state is None:
        return None, alerts, None

    if state.last_status in TERMINAL:
        if not was_terminal and state.breached and state.last_status == "DELIVERED":
            alerts.append(
                Alert(
                    order_id, state.zone, state.restaurant_id, "DELIVERED_AFTER_BREACH",
                    state.last_status, state.start_ms, state.last_event_ms,
                    (state.last_event_ms - state.start_ms) / 1000.0,
                )
            )
        state.terminal = True
        return state, alerts, _clamp_timeout(state.last_event_ms + ttl_ms, watermark_ms)

    deadline = state.start_ms + (ttl_ms if state.breached else sla_ms)
    return state, alerts, _clamp_timeout(deadline, watermark_ms)


def on_timeout(
    order_id: str,
    state: OrderSlaState,
    *,
    sla_ms: int,
    ttl_ms: int,
    watermark_ms: int,
) -> tuple[OrderSlaState | None, list[Alert], int | None]:
    """Called when the watermark passes the order's timer."""
    if state.terminal or state.breached:
        return None, [], None  # TTL expired: drop state to keep it bounded

    state.breached = True
    alert = Alert(
        order_id, state.zone, state.restaurant_id, "SLA_BREACH", state.last_status,
        state.start_ms, watermark_ms, (watermark_ms - state.start_ms) / 1000.0,
    )
    return state, [alert], _clamp_timeout(state.start_ms + ttl_ms, watermark_ms)


def spark_state_func(sla_ms: int, ttl_ms: int):
    """Adapter for ``groupBy("order_id").applyInPandasWithState``."""
    import pandas as pd  # imported lazily: only needed on Spark executors

    columns = [
        "order_id", "zone", "restaurant_id", "alert_type", "last_status",
        "placed_ts", "detected_at", "open_seconds",
    ]

    def to_frame(alerts: list[Alert]) -> pd.DataFrame:
        df = pd.DataFrame([a.__dict__ for a in alerts])
        df["placed_ts"] = pd.to_datetime(df.pop("placed_ms"), unit="ms")
        df["detected_at"] = pd.to_datetime(df.pop("detected_ms"), unit="ms")
        return df[columns]

    def func(key, pdfs, group_state):
        order_id = key[0]
        watermark_ms = group_state.getCurrentWatermarkMs()
        current = OrderSlaState.from_tuple(group_state.get) if group_state.exists else None

        if group_state.hasTimedOut:
            new_state, alerts, timeout = on_timeout(
                order_id, current, sla_ms=sla_ms, ttl_ms=ttl_ms, watermark_ms=watermark_ms
            )
        else:
            events = []
            for pdf in pdfs:
                for status, event_ts, zone, restaurant_id in zip(
                    pdf["status"], pdf["event_ts"], pdf["zone"], pdf["restaurant_id"], strict=True
                ):
                    events.append({
                        "status": status,
                        "event_ms": pd.Timestamp(event_ts).value // 1_000_000,
                        "zone": zone,
                        "restaurant_id": restaurant_id,
                    })
            new_state, alerts, timeout = on_events(
                order_id, current, events, sla_ms=sla_ms, ttl_ms=ttl_ms, watermark_ms=watermark_ms
            )

        if new_state is None:
            group_state.remove()
        else:
            group_state.update(new_state.to_tuple())
            group_state.setTimeoutTimestamp(timeout)

        if alerts:
            yield to_frame(alerts)

    return func
