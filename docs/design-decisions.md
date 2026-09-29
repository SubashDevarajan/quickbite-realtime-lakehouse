# Design decisions and trade-offs

Each section says what was chosen, what was rejected, and when the choice would change.

## Bronze stores raw bytes, not parsed rows

**Chosen:** bronze is an exact copy of Kafka: key, value bytes, topic, partition, offset, broker timestamp.

**Why:** Kafka retention is finite (7 days here). If silver has a bug, or a new data-quality rule is
added, silver must be rebuildable after Kafka has deleted the data. Parsing in bronze would bake
the parser's bugs into the only durable copy. Offsets also give per-row lineage, so any silver
row can be traced to one Kafka record.

**Cost:** storage for data that is also stored in silver. In production, bronze gets a longer
retention and cheaper storage tier than silver.

## Decoding Avro per schema id inside `foreachBatch`

Avro binary cannot be decoded without the exact writer schema. During a rollout the topic holds
v1 and v2 records together, so `from_avro` with one fixed schema would fail or misread half of
them. Each micro-batch collects its distinct schema ids (a tiny `collect`), fetches each schema
from the registry once (ids are immutable, so they are cached forever), decodes each group with
its own schema, and aligns every version to the latest shape.

**Rejected:** decoding in a Python UDF with `fastavro`. That works, but it moves every record
through Python serialisation and is several times slower than the native `from_avro`.

**Rejected:** Databricks' `from_avro(..., schemaRegistryAddress)`. It does this natively, but only
on Databricks, and this project must run anywhere.

## Status is a `string`, not an Avro `enum`

Adding a symbol to an Avro enum is a breaking change for old readers. New order states will
appear (`READY_FOR_PICKUP`, `RETURNED`...), so status is a string and validated in silver, where
unknown values go to the DLQ instead of crashing a consumer.

## Money is `decimal(10,2)`

Never `double` for currency: `0.1 + 0.2 != 0.3`. The Avro `decimal` logical type maps straight to
Spark `DecimalType(10,2)`, and GMV sums as `decimal(20,2)`.

## Two de-duplication strategies

| | Orders: insert-only MERGE on `event_id` | Clicks: `dropDuplicatesWithinWatermark` |
|---|---|---|
| Horizon | Unbounded: a duplicate a week later is still caught | Only within the watermark (10 min) |
| State | None in Spark; the Delta table is the "state" | Event ids in RocksDB, expired by the watermark |
| Cost | A MERGE per batch (pruned to the batch's `event_date` partitions) | Cheap append |
| When | Low-volume, high-value data where one duplicate matters (money) | High-volume data where duplicates arrive close together |

Duplicates from client retries arrive within seconds, so a 10-minute window catches them in
practice. For orders, one double-counted payment is expensive enough to justify the MERGE.

## Idempotency instead of distributed transactions

One silver micro-batch performs three writes (DLQ append, event MERGE, current-state MERGE),
which are three Delta commits, not one atomic transaction. Rather than coordinate them, each
write is made idempotent:

- **DLQ append**: Delta's `txnAppId` + `txnVersion=batch_id`. A replayed batch id is skipped.
- **Event MERGE**: insert-only on `event_id`. Replaying inserts nothing.
- **Current-state MERGE**: forward-only on status rank, with `GREATEST`/`LEAST`/`COALESCE`
  for other columns. Applying the same batch twice gives the same row.

If the job dies between commits, Spark replays the batch and the finished writes are no-ops.
That is the whole exactly-once argument, and `make chaos-kill-silver && make verify` tests it.

**Caveat:** `txnAppId` is tied to checkpoint batch ids. Deleting a checkpoint without deleting
its tables would make Delta skip the "already seen" batch ids. `scripts/reset_layers.py`
therefore always deletes tables and checkpoints together.

## Forward-only current state

A late `PICKED_UP` must not overwrite `DELIVERED`. Each status has a rank; the MERGE updates
status only if the incoming rank is higher, or equal with a later event time. `placed_ts` uses
`LEAST`, so a PLACED event arriving after later events still fills it in.

**Alternative:** rebuild current state from the full event history each batch. That's simpler to
reason about, but cost grows with history size.

## Watermark = 2 minutes

The watermark is the trade-off between **completeness** and **latency / state size**. A
1-minute window is emitted about 3 minutes after it opens (1 min window + 2 min watermark).
Events more than 2 minutes late are left out of `zone_metrics_1m`. They still exist in silver,
and `numRowsDroppedByWatermark` counts them, so the loss is measured, not silent. The
generator makes about half of its late events exceed the watermark, so drops are visible.

For finance-grade numbers, the streaming aggregate is the "fast" answer and a nightly batch
over silver is the "correct" one (lambda-style reconciliation). The table is append-only, so
the batch job can overwrite a day's partition.

### Two watermarks, on purpose

The SLA tracker and the conversion join use a longer **5-minute** watermark
(`ALERT_WATERMARK_DELAY`). For a metric, a dropped late event makes a count slightly low.
For these two queries it makes the answer *wrong*: a late `DELIVERED` dropped by the SLA
tracker would raise a false breach, and a late `PLACED` dropped by the join would mark a
converted checkout as abandoned. The watermark must exceed the worst expected lateness
(4 minutes in the generator). The price is that alerts fire up to 5 minutes after the
deadline rather than 2.

## SLA alerts need arbitrary state, not a window

"Alert if an order isn't delivered within N minutes" fires on the *absence* of an event.
Windows only emit when data arrives, so this uses `applyInPandasWithState` with an
**event-time** timeout. The timer is driven by the watermark rather than the wall clock, so
replaying old data gives the same alerts.

State is bounded: after a terminal status or a breach, the order is kept as a tombstone for a
TTL (so a straggling event cannot re-open it and cause a false alert), then removed.

The state machine is plain Python (`pipeline/state/order_sla.py`) with unit tests that run
in milliseconds; the Spark adapter is a thin wrapper around it.

## Stream-stream join needs two watermarks and a time bound

Without watermarks on both sides and `order_ts BETWEEN checkout_ts AND checkout_ts + 5 min`,
Spark would have to keep every checkout forever in case an order shows up. With them, state is
dropped once the watermark passes the bound. That is also when an unmatched checkout is
emitted as `converted = false`, which is why abandoned checkouts appear a few minutes after
the fact.

## Trigger interval

A 10 s trigger was chosen for a laptop: each micro-batch has fixed overhead (planning, Delta
commit, and for silver three commits). In production, the knobs would be:

- **Lower latency:** 1–2 s triggers on a real cluster, fewer commits per batch (e.g. write the
  DLQ asynchronously), and Delta optimized writes.
- **Lower cost:** `trigger(availableNow=True)` every few minutes from a scheduler. It uses
  the same code and checkpoints, costs a fraction of always-on, and gives minutes of latency.

## One job per layer, one checkpoint per query

Each layer is its own process, and each query has its own checkpoint, so:

- a gold bug can be fixed and gold replayed without touching silver,
- a crash in one query doesn't restart the others,
- each layer can be sized separately.

**Cost:** 3 JVMs on a laptop. On Databricks these would be three tasks in one Workflow on a
shared job cluster.

## Delta over Iceberg / Hudi

Delta has first-class Structured Streaming support (a table as a streaming source and sink,
idempotent `foreachBatch` writes via `txnAppId`), and MERGE and OPTIMIZE/ZORDER in open
source. Iceberg would be the choice for multi-engine access (Trino, Flink, Snowflake reading
the same tables). With Delta UniForm, both are possible.

## Spark Structured Streaming over Flink

Flink offers true per-event processing and richer state and timers. Structured Streaming wins
here because the same engine, API and team skills cover batch (backfills, maintenance)
and streaming, and seconds of latency is enough for these use cases. For sub-second
fraud detection, Flink would be the better choice.

## What I'd change at 100× scale

- **Partitioning:** `orders_current` MERGE scans the table. At scale, cluster it by `order_id`
  (Delta liquid clustering) or partition by order date and include it in the merge key.
- **Kafka:** increase partitions for parallelism, `replication.factor=3`, `min.insync.replicas=2`,
  and tiered storage for longer retention.
- **State:** state store on fast local SSD, changelog checkpointing (already on), and
  monitoring of `state_rows` growth.
- **DLQ:** alert on DLQ rate per reason. Add a re-drive job that re-processes DLQ rows after a fix.
- **Data contracts:** schema compatibility checks in the producer's CI (the `schema-check`
  drill), plus column-level expectations published with the table.
