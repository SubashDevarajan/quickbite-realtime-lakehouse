# Runbook

How to recognise and recover from the failures this platform is built to survive.
Every scenario can be reproduced locally; the `make` target is given where one exists.

## A streaming job crashed

**Symptoms:** a container restarts (`make ps` shows a restart count); `query terminated`
appears in `make logs`.

**What happens automatically:** `restart: unless-stopped` restarts the job. It reads its
checkpoint, finds the last *committed* batch, and re-runs the next one. Bronze and silver
writes are idempotent, so the re-run does not duplicate anything.

**Check:** `make verify` after the stream drains. Reproduce with `make chaos-kill-silver`.

## Consumer lag is growing

**Symptoms:**
- Kafka UI shows lag rising on the topics.
- In `make logs`, `input rows/s` exceeds `processed rows/s` for several batches.
- `batch_duration_ms` is above the trigger interval.

**Actions:**
1. Check which query is slow (dashboard → *Pipeline health*).
2. If `batch_duration_ms` has jumped, check the small-file count and run `make maintain`.
3. If it's steady load, give the job more cores (`SPARK_MASTER=local[8]`) or, on a cluster,
   more executors. Kafka partitions (6) cap bronze parallelism.
4. `maxOffsetsPerTrigger` bounds each batch, so catching up happens in predictable steps
   rather than one huge batch.

Reproduce with `make chaos-pause-silver`.

## Dead-letter rate spikes

**Symptoms:** the DLQ panel shows a jump for one `primary_reason`.

| Reason | Usual cause | Action |
|---|---|---|
| `NOT_CONFLUENT_AVRO` | A producer is writing without the Avro serializer | Find it by `kafka_partition`/`kafka_offset` → key in Kafka UI |
| `UNKNOWN_SCHEMA_ID` | Records from a different registry (wrong env) | Check the producer's registry URL |
| `AVRO_DECODE_ERROR` | Corrupt payload, or schema id mismatch | Inspect `raw_value` in the DLQ |
| `UNKNOWN_STATUS` | A new status was shipped upstream | Add it to `ORDER_STATUSES` + `STATUS_RANK`, then re-drive (below) |
| `FUTURE_EVENT_TS` | Device clock skew | Tune `MAX_FUTURE_SKEW_SECONDS` if legitimate |

**Re-drive after a fix:** DLQ rows keep the original bytes and Kafka coordinates, and bronze
keeps everything, so the simplest correct re-drive is `make replay` (rebuild silver and gold
from bronze with the fixed code).

## A breaking schema was registered or produced

**Prevention:** the subject is set to BACKWARD compatibility, so the registry rejects the
schema (`make schema-check` shows this).

**If bad data got through anyway** (e.g. compatibility was switched off): the records land in
the DLQ as decode or DQ errors. Nothing is lost, because bronze has the raw bytes. Fix the
decoder, then `make replay`.

## Silver or gold logic had a bug

1. Fix the code (the job containers mount `./pipeline`, so there's no rebuild needed).
2. `make replay`. It stops silver and gold, deletes their tables *and* checkpoints together,
   restarts, and rebuilds from bronze.
3. `make verify`.

Never delete a checkpoint without its table, or a table without its checkpoint. Delta
idempotency (`txnAppId`) is keyed on batch ids stored in the checkpoint.

## Checkpoint is corrupted or incompatible after an upgrade

Some query changes are incompatible with an existing checkpoint (changing a stateful
operator's keys, aggregation or watermark). Treat it like a logic bug: `make replay` for
silver and gold. For bronze, reset its checkpoint with `STARTING_OFFSETS=earliest`. Bronze
will re-ingest what Kafka still retains (creating duplicates in bronze), and silver's
de-duplication absorbs them.

## Too many small files / queries getting slower

**Symptoms:** reads of silver slow down over hours, and `numFiles` in `DESCRIBE DETAIL`
reaches the thousands.

**Action:** `make maintain` (OPTIMIZE + ZORDER + VACUUM). It is safe to run while streams
are running. If it collides with a MERGE, the silver batch retries on the Delta conflict
automatically (`_retry_on_conflict`). Schedule it hourly in production.

## Late data is being dropped

**Symptoms:** `dropped_late` is non-zero in *Pipeline health*.

This is expected. Events later than the watermark (2 min) are excluded from windowed
aggregates. They are **still in silver**. If the drop rate is too high for the business,
raise `WATERMARK_DELAY`. That costs latency, since windows finalise later, and memory, since
more state is kept. Then replay gold.
