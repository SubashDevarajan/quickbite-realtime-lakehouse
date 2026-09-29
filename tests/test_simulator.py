"""Pure-Python tests for the event simulator (no Kafka, no Spark)."""

from __future__ import annotations

import random
from collections import defaultdict

from generator.simulator import CLICK_TOPIC, ORDER_TOPIC, SimConfig, Simulator

START = 1_700_000_000_000
VALID_SEQUENCES = [
    ["PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED"],
    ["PLACED", "CANCELLED"],
]


def run(seed: int = 7, minutes: int = 20, schema_version: int = 2, **cfg):
    sim = Simulator(SimConfig(sessions_per_sec=3, **cfg), random.Random(seed), START)
    sim.schema_version = schema_version
    out = []
    for step in range(1, minutes * 60 * 4 + 1):  # 250 ms ticks
        out.extend(sim.tick(START + step * 250))
    return out


def test_is_deterministic_for_a_seed():
    a, b = run(seed=42, minutes=3), run(seed=42, minutes=3)
    assert [e.value for e in a] == [e.value for e in b]


def test_event_ids_are_unique():
    events = run()
    ids = [e.value["event_id"] for e in events]
    assert len(ids) == len(set(ids))


def test_order_lifecycles_are_valid_prefixes():
    by_order = defaultdict(list)
    for e in run():
        if e.topic == ORDER_TOPIC:
            by_order[e.value["order_id"]].append(e.value)
    assert len(by_order) > 50

    for events in by_order.values():
        statuses = [ev["status"] for ev in events]
        timestamps = [ev["event_ts"] for ev in events]
        assert timestamps == sorted(timestamps)
        assert any(seq[: len(statuses)] == statuses for seq in VALID_SEQUENCES), statuses


def test_some_orders_get_stuck_and_some_complete():
    last_status = {}
    for e in run(minutes=30):
        if e.topic == ORDER_TOPIC:
            last_status[e.value["order_id"]] = e.value["status"]
    finals = set(last_status.values())
    assert {"DELIVERED", "CANCELLED"} <= finals
    assert finals & {"ACCEPTED", "PICKED_UP"}  # stuck (or still in flight)


def test_every_order_follows_a_checkout_in_the_same_session():
    checkout_ts = {}
    orders = []
    for e in run():
        if e.topic == CLICK_TOPIC and e.value["event_type"] == "checkout_started":
            checkout_ts[e.value["session_id"]] = e.value["event_ts"]
        elif e.topic == ORDER_TOPIC and e.value["status"] == "PLACED":
            orders.append(e.value)
    assert orders
    for o in orders:
        assert o["session_id"] in checkout_ts
        assert o["event_ts"] >= checkout_ts[o["session_id"]]


def test_schema_v1_omits_payment_method_and_v2_includes_it():
    v1 = [e.value for e in run(minutes=3, schema_version=1) if e.topic == ORDER_TOPIC]
    v2 = [e.value for e in run(minutes=3, schema_version=2) if e.topic == ORDER_TOPIC]
    assert v1 and all("payment_method" not in v for v in v1)
    assert v2 and all(v["payment_method"] in {"UPI", "CARD", "COD", "WALLET"} for v in v2)


def test_amounts_are_two_decimal_places():
    for e in run(minutes=3):
        if e.topic == ORDER_TOPIC:
            assert e.value["amount"].as_tuple().exponent == -2
