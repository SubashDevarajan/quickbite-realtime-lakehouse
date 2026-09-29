"""Gold transformations, exercised on static DataFrames.

``withWatermark`` is a no-op on a batch DataFrame, so the windowing, join and
sessionisation logic can be checked deterministically without running a
stream. Watermark/late-data behaviour is covered by the end-to-end drill
(``make verify``) and the SLA state tests."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from tests.helpers import BASE_TS

pytestmark = pytest.mark.spark


def ts(seconds: float):
    return BASE_TS + timedelta(seconds=seconds)


def order_events(spark, rows):
    cols = "order_id string, customer_id string, session_id string, zone string, restaurant_id string, " \
           "status string, amount decimal(10,2), event_ts timestamp"
    return spark.createDataFrame(rows, cols)


def clicks(spark, rows):
    cols = "event_id string, session_id string, customer_id string, event_type string, " \
           "restaurant_id string, zone string, device string, event_ts timestamp"
    return spark.createDataFrame(rows, cols)


def test_zone_metrics_1m(spark):
    from pipeline.gold import zone_metrics_1m

    ev = order_events(spark, [
        ("O1", "C1", "S1", "HEBBAL", "R1", "PLACED", Decimal("100.00"), ts(5)),
        ("O2", "C2", "S2", "HEBBAL", "R1", "PLACED", Decimal("50.50"), ts(30)),
        ("O2", "C2", "S2", "HEBBAL", "R1", "CANCELLED", Decimal("50.50"), ts(50)),
        ("O1", "C1", "S1", "HEBBAL", "R1", "DELIVERED", Decimal("100.00"), ts(70)),
        ("O3", "C3", "S3", "JAYANAGAR", "R2", "PLACED", Decimal("10.00"), ts(10)),
    ])
    rows = {(r["zone"], r["window_start"]): r for r in zone_metrics_1m(ev, "2 minutes").collect()}
    first = rows[("HEBBAL", ts(0).replace(tzinfo=None))]
    assert first["orders_placed"] == 2
    assert first["gmv"] == Decimal("150.50")
    assert first["orders_cancelled"] == 1
    assert first["unique_customers"] == 2
    second = rows[("HEBBAL", ts(60).replace(tzinfo=None))]
    assert second["orders_placed"] == 0 and second["orders_delivered"] == 1 and second["gmv"] == 0
    assert rows[("JAYANAGAR", ts(0).replace(tzinfo=None))]["orders_placed"] == 1


def test_checkout_conversion_left_outer_with_time_bound(spark):
    from pipeline.gold import checkout_conversion

    c = clicks(spark, [
        ("E1", "S1", "C1", "checkout_started", "R1", "HEBBAL", "ios", ts(0)),
        ("E2", "S2", "C2", "checkout_started", "R1", "HEBBAL", "web", ts(0)),
        ("E3", "S3", "C3", "checkout_started", "R1", "HEBBAL", "web", ts(0)),
        ("E4", "S1", "C1", "search", None, "HEBBAL", "ios", ts(-30)),
    ])
    o = order_events(spark, [
        ("O1", "C1", "S1", "HEBBAL", "R1", "PLACED", Decimal("99.00"), ts(12)),  # converted
        ("O1", "C1", "S1", "HEBBAL", "R1", "ACCEPTED", Decimal("99.00"), ts(20)),  # not a PLACED
        ("O3", "C3", "S3", "HEBBAL", "R1", "PLACED", Decimal("10.00"), ts(900)),  # outside window
    ])
    rows = {r["session_id"]: r for r in checkout_conversion(c, o, "2 minutes", 300).collect()}
    assert set(rows) == {"S1", "S2", "S3"}  # only checkout events, one row each
    assert rows["S1"]["converted"] and rows["S1"]["order_id"] == "O1"
    assert rows["S1"]["seconds_to_order"] == pytest.approx(12.0)
    assert not rows["S2"]["converted"]
    assert not rows["S3"]["converted"]


def test_user_sessions_split_on_inactivity_gap(spark):
    from pipeline.gold import user_sessions

    c = clicks(spark, [
        ("E1", "Sx", "C1", "app_open", None, "HEBBAL", "ios", ts(0)),
        ("E2", "Sx", "C1", "view_restaurant", "R1", "HEBBAL", "ios", ts(60)),
        ("E3", "Sx", "C1", "add_to_cart", "R1", "HEBBAL", "ios", ts(100)),
        # 10 minutes of silence -> new session
        ("E4", "Sy", "C1", "app_open", None, "HEBBAL", "ios", ts(700)),
    ])
    sessions = sorted(user_sessions(c, "2 minutes", 180).collect(), key=lambda r: r["session_start"])
    assert len(sessions) == 2
    assert sessions[0]["events"] == 3
    assert sessions[0]["active_seconds"] == pytest.approx(100.0)
    assert sessions[0]["added_to_cart"] and not sessions[0]["reached_checkout"]
    assert sessions[1]["events"] == 1
