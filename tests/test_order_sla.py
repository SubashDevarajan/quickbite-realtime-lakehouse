"""Unit tests for the SLA state machine (plain Python, runs in milliseconds)."""

from __future__ import annotations

from pipeline.state.order_sla import OrderSlaState, on_events, on_timeout

SLA = 240_000
TTL = 900_000
T0 = 1_700_000_000_000


def ev(status: str, offset_s: float) -> dict:
    return {"status": status, "event_ms": T0 + int(offset_s * 1000), "zone": "HSR_LAYOUT", "restaurant_id": "R0001"}


def apply(state, *events, wm=0):
    return on_events("O-1", state, list(events), sla_ms=SLA, ttl_ms=TTL, watermark_ms=wm)


def timeout(state, wm):
    return on_timeout("O-1", state, sla_ms=SLA, ttl_ms=TTL, watermark_ms=wm)


def test_new_order_sets_timer_at_sla_deadline():
    state, alerts, t = apply(None, ev("PLACED", 0))
    assert alerts == []
    assert state.last_status == "PLACED" and state.placed_ms == T0
    assert t == T0 + SLA


def test_order_delivered_in_time_raises_no_alert_and_becomes_tombstone():
    state, _, _ = apply(None, ev("PLACED", 0), ev("ACCEPTED", 10))
    state, alerts, t = apply(state, ev("PICKED_UP", 60), ev("DELIVERED", 120))
    assert alerts == []
    assert state.terminal and state.last_status == "DELIVERED"
    assert t == T0 + 120_000 + TTL

    # TTL expiry drops the state silently.
    state, alerts, t = timeout(state, wm=t)
    assert state is None and alerts == [] and t is None


def test_breach_then_late_delivery():
    state, _, t = apply(None, ev("PLACED", 0), ev("ACCEPTED", 5))
    state, alerts, t2 = timeout(state, wm=t)
    assert [a.alert_type for a in alerts] == ["SLA_BREACH"]
    assert alerts[0].last_status == "ACCEPTED"
    assert alerts[0].open_seconds >= SLA / 1000
    assert state.breached and t2 == T0 + TTL

    state, alerts, _ = apply(state, ev("DELIVERED", 400), wm=t)
    assert [a.alert_type for a in alerts] == ["DELIVERED_AFTER_BREACH"]
    assert alerts[0].open_seconds == 400.0


def test_breached_order_is_dropped_after_ttl_without_second_alert():
    state, _, t = apply(None, ev("PLACED", 0))
    state, _, t = timeout(state, wm=t)
    state, alerts, _ = timeout(state, wm=t)
    assert state is None and alerts == []


def test_out_of_order_events_never_regress_status():
    state, alerts, _ = apply(None, ev("DELIVERED", 120), ev("PLACED", 0))
    state, alerts2, _ = apply(state, ev("PICKED_UP", 60))
    assert state.last_status == "DELIVERED"
    assert alerts == alerts2 == []


def test_straggler_after_terminal_does_not_reopen_order():
    state, _, _ = apply(None, ev("PLACED", 0), ev("CANCELLED", 20))
    state, alerts, _ = apply(state, ev("ACCEPTED", 10))
    assert state.terminal and state.last_status == "CANCELLED"
    assert alerts == []


def test_missing_placed_uses_first_seen_event_as_start():
    state, _, t = apply(None, ev("ACCEPTED", 30))
    assert state.placed_ms is None
    assert state.start_ms == T0 + 30_000
    assert t == T0 + 30_000 + SLA


def test_late_placed_event_moves_the_deadline_earlier():
    state, _, _ = apply(None, ev("ACCEPTED", 30))
    state, _, t = apply(state, ev("PLACED", 0))
    assert state.placed_ms == T0 and t == T0 + SLA


def test_timer_is_never_set_behind_the_watermark():
    watermark = T0 + 10 * SLA
    _, _, t = apply(None, ev("PLACED", 0), wm=watermark)
    assert t == watermark + 1  # fires on the next batch -> immediate breach alert


def test_state_roundtrips_through_tuple():
    state, _, _ = apply(None, ev("PLACED", 0))
    assert OrderSlaState.from_tuple(state.to_tuple()) == state


# ---------------------------------------------------------------- adapter
class FakeGroupState:
    """Mimics pyspark GroupState closely enough to exercise the adapter."""

    def __init__(self, value=None, timed_out=False, watermark=0):
        self._value, self.hasTimedOut, self._wm = value, timed_out, watermark
        self.timeout = None
        self.removed = False

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self._value = value

    def remove(self):
        self._value, self.removed = None, True

    def setTimeoutTimestamp(self, ts):
        self.timeout = ts

    def getCurrentWatermarkMs(self):
        return self._wm


def test_spark_adapter_emits_alert_frames():
    import pandas as pd

    from pipeline.state.order_sla import spark_state_func

    func = spark_state_func(SLA, TTL)
    gs = FakeGroupState()
    pdf = pd.DataFrame({
        "status": ["PLACED"],
        "event_ts": [pd.to_datetime(T0, unit="ms")],
        "zone": ["HSR_LAYOUT"],
        "restaurant_id": ["R0001"],
    })
    assert list(func(("O-1",), iter([pdf]), gs)) == []
    assert gs.timeout == T0 + SLA and gs.exists

    gs.hasTimedOut, gs._wm = True, T0 + SLA + 1
    frames = list(func(("O-1",), iter([]), gs))
    assert len(frames) == 1
    row = frames[0].iloc[0]
    assert row["alert_type"] == "SLA_BREACH" and row["order_id"] == "O-1"
    assert row["placed_ts"] == pd.to_datetime(T0, unit="ms")
    assert list(frames[0].columns) == [
        "order_id", "zone", "restaurant_id", "alert_type", "last_status",
        "placed_ts", "detected_at", "open_seconds",
    ]
