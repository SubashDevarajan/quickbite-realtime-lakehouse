"""End-to-end silver micro-batch: DLQ routing, de-duplication, idempotent
retries and out-of-order-safe current state, against real Delta tables."""

from __future__ import annotations

import pytest

from tests.helpers import avro_schema, bronze_df, confluent_avro, order

pytestmark = pytest.mark.spark


@pytest.fixture
def silver(spark, lake_paths):
    pytest.importorskip("fastavro")
    from pipeline.avro_decode import StaticSchemaResolver
    from pipeline.silver import process_order_batch
    from pipeline.tables import ensure_silver

    ensure_silver(spark, lake_paths)
    resolver = StaticSchemaResolver({1: avro_schema(1), 2: avro_schema(2)})
    offset = {"next": 0}

    def run(records_or_bytes, batch_id):
        values = [
            v if isinstance(v, bytes) else confluent_avro(v, schema_id=2, version=2)
            for v in records_or_bytes
        ]
        df = bronze_df(spark, "order_events", values, start_offset=offset["next"], kafka_offset_s=offset["next"])
        offset["next"] += len(values)
        return process_order_batch(spark, df, batch_id, paths=lake_paths, resolver=resolver,
                                   max_future_skew_seconds=300)

    def table(path):
        return spark.read.format("delta").load(path)

    return run, table, lake_paths


def test_duplicates_bad_records_and_retries(silver):
    run, table, p = silver
    batch = [
        order("E1", "O1", "PLACED", 0),
        order("E1", "O1", "PLACED", 0),  # duplicate within the batch
        order("E2", "O1", "ACCEPTED", 10),
        order("E3", "O2", "TELEPORTED", 0),  # DQ failure
        b"garbage-bytes-not-avro",  # undecodable
    ]
    run(batch, batch_id=0)
    assert table(p.silver_order_events).count() == 2
    assert table(p.dead_letter).count() == 2
    reasons = {r["primary_reason"] for r in table(p.dead_letter).collect()}
    assert reasons == {"UNKNOWN_STATUS", "NOT_CONFLUENT_AVRO"}

    # Spark retries the same micro-batch after a crash: nothing may double.
    run(batch, batch_id=0)
    assert table(p.silver_order_events).count() == 2
    assert table(p.dead_letter).count() == 2

    # The same event arriving again in a *later* batch is also dropped.
    run([order("E2", "O1", "ACCEPTED", 10)], batch_id=1)
    assert table(p.silver_order_events).count() == 2

    current = table(p.silver_orders_current).collect()
    assert len(current) == 1 and current[0]["status"] == "ACCEPTED"


def test_out_of_order_events_do_not_regress_current_state(silver):
    run, table, p = silver
    run([order("E3", "O9", "DELIVERED", 120)], batch_id=0)
    run([order("E2", "O9", "PICKED_UP", 60), order("E1", "O9", "PLACED", 0)], batch_id=1)

    row = table(p.silver_orders_current).filter("order_id = 'O9'").first()
    assert row["status"] == "DELIVERED"
    assert row["status_rank"] == 4
    assert row["placed_ts"] is not None  # filled in when the late PLACED arrived
    assert row["last_event_ts"] > row["placed_ts"]
    assert table(p.silver_order_events).filter("order_id = 'O9'").count() == 3


def test_schema_v1_and_v2_land_in_the_same_table(silver):
    run, table, p = silver
    v1 = confluent_avro(order("E10", "O10", "PLACED", 0), schema_id=1, version=1)
    v2 = confluent_avro(order("E11", "O11", "PLACED", 0, payment_method="CARD"), schema_id=2, version=2)
    run([v1, v2], batch_id=0)
    rows = {r["event_id"]: r for r in table(p.silver_order_events).collect()}
    assert rows["E10"]["payment_method"] is None and rows["E10"]["schema_id"] == 1
    assert rows["E11"]["payment_method"] == "CARD" and rows["E11"]["schema_id"] == 2
