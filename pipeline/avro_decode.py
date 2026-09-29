"""Decode Confluent-framed Avro inside Spark, one schema version at a time.

Confluent wire format::

    byte 0      magic byte, always 0x00
    bytes 1-4   schema id (big-endian int) in the Schema Registry
    bytes 5..   Avro binary payload written with that schema

Avro binary can only be decoded with the exact *writer* schema, so a topic
that carries several schema versions (v1 and v2 during a rollout) cannot be
decoded with a single fixed schema. Per micro-batch we:

1. split the frame into (magic, schema_id, payload) with Spark SQL functions,
2. look up each distinct schema id in the registry (cached; ids are immutable),
3. decode each id's rows with ``from_avro`` using its own writer schema,
4. align every version to the canonical (latest) shape, missing fields = NULL,
5. union the parts back together.

Rows that cannot be decoded are *kept* with a ``decode_error`` reason so they
can be routed to the dead-letter queue instead of failing the stream.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType

from pipeline.schemas import ORDER_EVENT_SCHEMA

NOT_CONFLUENT_AVRO = "NOT_CONFLUENT_AVRO"
UNKNOWN_SCHEMA_ID = "UNKNOWN_SCHEMA_ID"
AVRO_DECODE_ERROR = "AVRO_DECODE_ERROR"


class SchemaResolver(Protocol):
    def get(self, schema_id: int) -> str | None:
        """Return the Avro schema JSON for ``schema_id`` or None if unknown."""


class RegistrySchemaResolver:
    """Fetches writer schemas from Confluent Schema Registry, with caching."""

    def __init__(self, url: str, timeout_s: float = 5.0):
        self.url = url.rstrip("/")
        self.timeout_s = timeout_s
        self._cache: dict[int, str | None] = {}

    def get(self, schema_id: int) -> str | None:
        if schema_id not in self._cache:
            try:
                with urllib.request.urlopen(
                    f"{self.url}/schemas/ids/{schema_id}", timeout=self.timeout_s
                ) as resp:
                    self._cache[schema_id] = json.loads(resp.read())["schema"]
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    self._cache[schema_id] = None  # genuinely unknown id
                else:
                    raise  # registry unavailable: fail the batch, Spark retries it
        return self._cache[schema_id]


class StaticSchemaResolver:
    """In-memory resolver for tests."""

    def __init__(self, schemas: dict[int, str]):
        self.schemas = schemas

    def get(self, schema_id: int) -> str | None:
        return self.schemas.get(schema_id)


def split_confluent_frame(df: DataFrame, value_col: str = "value") -> DataFrame:
    v = F.col(value_col)
    is_framed = (F.length(v) > 5) & (F.hex(F.substring(v, 1, 1)) == F.lit("00"))
    return df.withColumn(
        "schema_id",
        F.when(is_framed, F.conv(F.hex(F.substring(v, 2, 4)), 16, 10).cast("int")),
    ).withColumn(
        "avro_payload",
        F.when(is_framed, F.expr(f"substring({value_col}, 6, length({value_col}) - 5)")),
    )


def _align(struct_col: str, source: StructType, target: StructType) -> list:
    present = set(source.fieldNames())
    return [
        (F.col(f"{struct_col}.{f.name}") if f.name in present else F.lit(None))
        .cast(f.dataType)
        .alias(f.name)
        for f in target.fields
    ]


def decode_confluent_avro(
    df: DataFrame,
    resolver: SchemaResolver,
    target: StructType = ORDER_EVENT_SCHEMA,
    value_col: str = "value",
) -> DataFrame:
    """Return ``df``'s columns + decoded fields aligned to ``target`` +
    ``schema_id`` + ``decode_error`` (NULL when decoding succeeded).

    Must be called on a *static* DataFrame (e.g. inside ``foreachBatch``)
    because it collects the distinct schema ids of the batch.
    """
    framed = split_confluent_frame(df, value_col)
    passthrough = [F.col(c) for c in df.columns] + [F.col("schema_id")]

    ids = sorted(
        r["schema_id"]
        for r in framed.select("schema_id").distinct().collect()
        if r["schema_id"] is not None
    )
    known = {sid: resolver.get(sid) for sid in ids}
    known = {sid: s for sid, s in known.items() if s is not None}

    null_fields = [F.lit(None).cast(f.dataType).alias(f.name) for f in target.fields]

    # Rows we cannot even attempt to decode (not framed, or unknown schema id).
    undecodable = framed.filter(
        F.col("schema_id").isNull() | ~F.col("schema_id").isin(list(known) or [-1])
    ).select(
        *passthrough,
        *null_fields,
        F.when(F.col("schema_id").isNull(), F.lit(NOT_CONFLUENT_AVRO))
        .otherwise(F.lit(UNKNOWN_SCHEMA_ID))
        .alias("decode_error"),
    )

    parts = [undecodable]
    for sid, schema_json in known.items():
        decoded = framed.filter(F.col("schema_id") == sid).withColumn(
            "_evt", from_avro(F.col("avro_payload"), schema_json, {"mode": "PERMISSIVE"})
        )
        evt_type = decoded.schema["_evt"].dataType
        parts.append(
            decoded.select(
                *passthrough,
                *_align("_evt", evt_type, target),
                F.when(F.col("_evt").isNull(), F.lit(AVRO_DECODE_ERROR)).alias("decode_error"),
            )
        )

    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result
