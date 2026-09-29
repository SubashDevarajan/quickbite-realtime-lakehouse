"""Builders for bronze-shaped test data."""

from __future__ import annotations

import io
import json
import struct
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

SCHEMAS = Path(__file__).resolve().parent.parent / "schemas"
# Timezone-aware so results do not depend on the machine's local timezone.
BASE_TS = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
BASE_MS = int(BASE_TS.timestamp() * 1000)


def avro_schema(version: int) -> str:
    return (SCHEMAS / f"order_event_v{version}.avsc").read_text()


def confluent_avro(record: dict, schema_id: int, version: int) -> bytes:
    """Serialise ``record`` exactly like Confluent's AvroSerializer does."""
    import fastavro

    parsed = fastavro.parse_schema(json.loads(avro_schema(version)))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def order(event_id: str, order_id: str, status: str, offset_s: int, *, amount="250.00",
          zone="KORAMANGALA", session_id="S-1", payment_method=None) -> dict:
    rec = {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": "C000001",
        "session_id": session_id,
        "restaurant_id": "R0001",
        "zone": zone,
        "status": status,
        "amount": None if amount is None else Decimal(amount),
        "event_ts": BASE_MS + offset_s * 1000,
    }
    if payment_method is not None:
        rec["payment_method"] = payment_method
    return rec


BRONZE_COLUMNS = ["topic", "kafka_partition", "kafka_offset", "kafka_ts", "key", "value", "ingested_at", "ingest_date"]


def bronze_rows(topic: str, values: list[bytes], *, start_offset: int = 0, kafka_offset_s: int = 0) -> list[tuple]:
    """Bronze rows; kafka_ts is BASE_TS + kafka_offset_s + row index seconds."""
    return [
        (
            topic,
            0,
            start_offset + i,
            BASE_TS + timedelta(seconds=kafka_offset_s + i),
            b"k",
            bytearray(v),
            BASE_TS + timedelta(seconds=kafka_offset_s + i, milliseconds=500),
            date(2026, 1, 15),
        )
        for i, v in enumerate(values)
    ]


def bronze_df(spark, topic: str, values: list[bytes], **kwargs):
    from pipeline.tables import BRONZE_EVENTS

    return spark.createDataFrame(bronze_rows(topic, values, **kwargs), BRONZE_EVENTS)
