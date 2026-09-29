"""Pure-Python tests for chaos injection."""

from __future__ import annotations

import random
from collections import Counter

from generator.chaos import ChaosConfig, ChaosInjector
from generator.simulator import ORDER_TOPIC, SimConfig, Simulator

START = 1_700_000_000_000


def simulate(chaos_cfg: ChaosConfig, minutes: int = 10, seed: int = 3):
    rng = random.Random(seed)
    sim = Simulator(SimConfig(sessions_per_sec=3), rng, START)
    chaos = ChaosInjector(chaos_cfg, rng)
    originals, sent = [], []
    now = START
    for step in range(1, minutes * 60 * 4 + 1):
        now = START + step * 250
        batch = sim.tick(now)
        originals.extend(batch)
        sent.extend(chaos.apply(batch, now))
    sent.extend(chaos.flush())
    return originals, sent


def test_disabled_chaos_is_a_passthrough():
    originals, sent = simulate(ChaosConfig(enabled=False), minutes=3)
    assert [e.value for e in sent] == [e.value for e in originals]


def test_no_event_is_lost_and_counts_reconcile():
    originals, sent = simulate(ChaosConfig(duplicate_rate=0.05, late_rate=0.05, malformed_rate=0.02))
    expected = [e for e in sent if e.expected_in_silver]
    # Exactly one "expected" copy of every original event survives chaos.
    assert sorted(e.value["event_id"] for e in expected) == sorted(e.value["event_id"] for e in originals)

    tags = Counter(e.tag.split(":")[0] for e in sent)
    assert tags["duplicate"] > 0 and tags["late"] > 0 and tags["malformed"] > 0


def test_duplicates_reuse_the_event_id():
    _, sent = simulate(ChaosConfig(duplicate_rate=0.2, late_rate=0, malformed_rate=0), minutes=2)
    dup_ids = {e.value["event_id"] for e in sent if e.tag == "duplicate"}
    first_ids = {e.value["event_id"] for e in sent if e.tag == "normal"}
    assert dup_ids and dup_ids <= first_ids


def test_late_events_keep_their_original_event_time_and_arrive_out_of_order():
    rng = random.Random(1)
    sim = Simulator(SimConfig(sessions_per_sec=5), rng, START)
    chaos = ChaosInjector(ChaosConfig(duplicate_rate=0, late_rate=0.3, malformed_rate=0,
                                      late_delay_s=(30, 60)), rng)
    arrivals = []  # (arrival_ms, event_ts, tag)
    for step in range(1, 4 * 60 * 3):
        now = START + step * 250
        for e in chaos.apply(sim.tick(now), now):
            arrivals.append((now, e.value["event_ts"], e.tag))
    late = [a for a in arrivals if a[2] == "late"]
    assert late
    assert all(arrival - event_ts >= 30_000 for arrival, event_ts, _ in late)
    event_times = [ts for _, ts, _ in arrivals]
    assert event_times != sorted(event_times)  # genuinely out of order


def test_malformed_records_are_flagged_and_varied():
    _, sent = simulate(ChaosConfig(duplicate_rate=0, late_rate=0, malformed_rate=0.2), minutes=3)
    bad = [e for e in sent if e.tag.startswith("malformed")]
    assert bad and not any(e.expected_in_silver for e in bad)
    reasons = {e.tag.split(":")[1] for e in bad}
    assert {"garbage_bytes", "unknown_status", "negative_amount", "broken_json"} <= reasons

    garbage = [e for e in bad if e.tag == "malformed:garbage_bytes"]
    assert all(e.topic == ORDER_TOPIC and isinstance(e.value, bytes) for e in garbage)
