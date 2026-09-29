"""Data-quality rules for order and click events."""

from __future__ import annotations

import json

import pytest

from tests.helpers import BASE_MS, avro_schema, bronze_df, confluent_avro, order

pytestmark = pytest.mark.spark


def _checked_orders(spark, records):
    from pipeline.avro_decode import StaticSchemaResolver
    from pipeline.silver import decode_and_validate_orders

    values = [confluent_avro(r, schema_id=2, version=2) for r in records]
    df = bronze_df(spark, "order_events", values)
    out = decode_and_validate_orders(df, StaticSchemaResolver({2: avro_schema(2)}), 300)
    return {r["event_id"]: r for r in out.collect()}


def test_order_rules(spark):
    pytest.importorskip("fastavro")
    future = order("E-future", "O5", "PLACED", 0)
    future["event_ts"] = BASE_MS + 24 * 3600 * 1000
    rows = _checked_orders(spark, [
        order("E-ok", "O1", "PLACED", 0),
        order("E-status", "O2", "TELEPORTED", 0),
        order("E-neg", "O3", "PLACED", 0, amount="-1.00"),
        order("E-zone", "O4", "ACCEPTED", 0, zone=""),
        future,
        order("E-noamt", "O6", "PLACED", 0, amount=None),
        order("E-noamt-ok", "O7", "ACCEPTED", 0, amount=None),
    ])
    assert rows["E-ok"]["is_valid"] and rows["E-ok"]["dq_errors"] == []
    assert rows["E-status"]["dq_errors"] == ["UNKNOWN_STATUS"]
    assert rows["E-neg"]["dq_errors"] == ["NEGATIVE_AMOUNT"]
    assert rows["E-zone"]["dq_errors"] == ["MISSING_ZONE"]
    assert rows["E-future"]["dq_errors"] == ["FUTURE_EVENT_TS"]
    assert rows["E-noamt"]["dq_errors"] == ["MISSING_AMOUNT_ON_PLACED"]
    assert rows["E-noamt-ok"]["is_valid"]  # amount optional after PLACED


def test_multiple_violations_are_all_reported(spark):
    pytest.importorskip("fastavro")
    rows = _checked_orders(spark, [order("E-multi", "O1", "TELEPORTED", 0, amount="-5.00", zone=" ")])
    assert set(rows["E-multi"]["dq_errors"]) == {"UNKNOWN_STATUS", "NEGATIVE_AMOUNT", "MISSING_ZONE"}
    assert not rows["E-multi"]["is_valid"]


def test_click_rules(spark):
    from pipeline.silver import parse_and_validate_clicks

    def click(**overrides):
        base = {"event_id": "E1", "session_id": "S1", "customer_id": "C1", "event_type": "search",
                "restaurant_id": None, "zone": "HEBBAL", "device": "ios", "event_ts": BASE_MS}
        base.update(overrides)
        return json.dumps({k: v for k, v in base.items() if v is not ...}).encode()

    values = [
        click(),
        click(event_id=...),  # key removed
        click(event_type="teleport"),
        b'{"event_id": "E9", "session_id": "S1", "customer',  # truncated JSON
    ]
    out = {r["kafka_offset"]: r for r in parse_and_validate_clicks(bronze_df(spark, "clickstream", values), 300).collect()}
    assert out[0]["is_valid"]
    assert out[1]["dq_errors"] == ["MISSING_EVENT_ID"]
    assert out[2]["dq_errors"] == ["UNKNOWN_EVENT_TYPE"]
    assert out[3]["dq_errors"] == ["MALFORMED_JSON"]
