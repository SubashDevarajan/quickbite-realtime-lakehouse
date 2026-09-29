"""Decoding Confluent-framed Avro with multiple schema versions in one batch."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from tests.helpers import BASE_TS, avro_schema, bronze_df, confluent_avro, order

pytestmark = pytest.mark.spark


def test_mixed_versions_garbage_and_unknown_ids(spark):
    pytest.importorskip("fastavro")
    from pipeline.avro_decode import (
        AVRO_DECODE_ERROR,
        NOT_CONFLUENT_AVRO,
        UNKNOWN_SCHEMA_ID,
        StaticSchemaResolver,
        decode_confluent_avro,
    )

    v1 = confluent_avro(order("E1", "O1", "PLACED", 0), schema_id=1, version=1)
    v2 = confluent_avro(order("E2", "O2", "PLACED", 5, payment_method="UPI"), schema_id=2, version=2)
    garbage = b"this is not avro at all"
    unknown = confluent_avro(order("E3", "O3", "PLACED", 0), schema_id=99, version=1)
    truncated = v1[:12]

    df = bronze_df(spark, "order_events", [v1, v2, garbage, unknown, truncated])
    resolver = StaticSchemaResolver({1: avro_schema(1), 2: avro_schema(2)})
    rows = {r["kafka_offset"]: r for r in decode_confluent_avro(df, resolver).collect()}

    assert rows[0]["decode_error"] is None
    assert rows[0]["schema_id"] == 1
    assert rows[0]["event_id"] == "E1"
    assert rows[0]["payment_method"] is None  # v1 aligned to v2 shape
    assert rows[0]["amount"] == Decimal("250.00")
    assert rows[0]["event_ts"] == BASE_TS.replace(tzinfo=None)

    assert rows[1]["decode_error"] is None
    assert rows[1]["schema_id"] == 2
    assert rows[1]["payment_method"] == "UPI"
    assert rows[1]["event_ts"] == (BASE_TS + timedelta(seconds=5)).replace(tzinfo=None)

    assert rows[2]["decode_error"] == NOT_CONFLUENT_AVRO
    assert rows[3]["decode_error"] == UNKNOWN_SCHEMA_ID
    assert rows[4]["decode_error"] == AVRO_DECODE_ERROR
    # bronze columns are carried through for lineage and the DLQ
    assert rows[2]["value"] == bytearray(garbage)


def test_empty_batch_keeps_the_schema(spark):
    from pipeline.avro_decode import StaticSchemaResolver, decode_confluent_avro

    out = decode_confluent_avro(bronze_df(spark, "order_events", []), StaticSchemaResolver({}))
    assert out.count() == 0
    assert {"event_id", "amount", "payment_method", "decode_error", "schema_id"} <= set(out.columns)
