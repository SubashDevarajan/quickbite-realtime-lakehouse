"""Kafka producer for the QuickBite simulator.

* order events -> Avro, Confluent wire format, schema registered in Schema Registry
* clickstream  -> JSON
* garbage      -> raw bytes (to exercise the dead-letter queue)

Starts on schema v1 and switches to v2 after ``--schema-v2-after`` seconds to
demonstrate live schema evolution. Writes a ground-truth manifest on exit that
``scripts/verify_exactly_once.py`` compares against the lakehouse.

Usage:
    python -m generator.producer --sessions-per-sec 5 --duration 600
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import time
from collections import Counter
from decimal import Decimal
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from generator.chaos import ChaosConfig, ChaosInjector
from generator.simulator import ORDER_TOPIC, Emission, SimConfig, Simulator

log = logging.getLogger("generator")
SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"


def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(type(obj))


class Stats:
    def __init__(self) -> None:
        self.sent: Counter[str] = Counter()
        self.unique_valid: Counter[str] = Counter()
        self.delivery_errors = 0

    def record(self, em: Emission) -> None:
        self.sent[f"{em.topic}:{em.tag.split(':')[0]}"] += 1
        if em.tag.startswith("malformed"):
            self.sent[f"{em.topic}:{em.tag}"] += 1
        if em.expected_in_silver:
            self.unique_valid[em.topic] += 1

    def as_dict(self) -> dict:
        return {
            "sent": dict(sorted(self.sent.items())),
            "unique_valid_events": dict(self.unique_valid),
            "delivery_errors": self.delivery_errors,
        }


def merge_manifest(path: Path, run: dict, runtime_sec: float) -> dict:
    """The lakehouse accumulates data across generator runs, so the ground
    truth must too: add this run's counters to any previous manifest."""
    previous = json.loads(path.read_text()) if path.exists() else {}
    merged: dict = {"runs": previous.get("runs", 0) + 1,
                    "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "runtime_sec": round(previous.get("runtime_sec", 0) + runtime_sec, 1)}
    for section in ("sent", "unique_valid_events"):
        total = Counter(previous.get(section, {}))
        total.update(run[section])
        merged[section] = dict(sorted(total.items()))
    merged["delivery_errors"] = previous.get("delivery_errors", 0) + run["delivery_errors"]
    return merged


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--sessions-per-sec", type=float, default=float(os.getenv("SESSIONS_PER_SEC", 5)))
    p.add_argument("--duration", type=float, default=float(os.getenv("DURATION_SEC", 0)),
                   help="Seconds to run; 0 = until stopped")
    p.add_argument("--schema-v2-after", type=float, default=float(os.getenv("SCHEMA_V2_AFTER_SEC", 90)),
                   help="Switch order events from schema v1 to v2 after N seconds (<0 = never)")
    p.add_argument("--no-chaos", action="store_true")
    p.add_argument("--duplicate-rate", type=float, default=0.03)
    p.add_argument("--late-rate", type=float, default=0.03)
    p.add_argument("--malformed-rate", type=float, default=0.01)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--manifest", default=os.getenv("MANIFEST_PATH", "data/generator/manifest.json"))
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    rng = random.Random(args.seed)

    sr = SchemaRegistryClient({"url": args.schema_registry})
    subject = f"{ORDER_TOPIC}-value"
    try:
        sr.set_compatibility(subject_name=subject, level="BACKWARD")
    except Exception as exc:  # subject-level config can be set before first registration
        log.warning("could not set compatibility on %s: %s", subject, exc)

    serializers = {
        v: AvroSerializer(sr, (SCHEMA_DIR / f"order_event_v{v}.avsc").read_text(),
                          conf={"auto.register.schemas": True})
        for v in (1, 2)
    }

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        # Idempotent producer: broker de-dupes producer retries, so any
        # duplicates downstream are the *intentional* chaos ones.
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "batch.size": 131072,
        "compression.type": "lz4",
    })

    stats = Stats()

    def on_delivery(err, _msg):
        if err is not None:
            stats.delivery_errors += 1
            log.error("delivery failed: %s", err)

    sim = Simulator(SimConfig(sessions_per_sec=args.sessions_per_sec), rng, int(time.time() * 1000))
    sim.schema_version = 1 if args.schema_v2_after != 0 else 2
    chaos = ChaosInjector(
        ChaosConfig(
            duplicate_rate=args.duplicate_rate,
            late_rate=args.late_rate,
            malformed_rate=args.malformed_rate,
            enabled=not args.no_chaos,
        ),
        rng,
    )

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    def send(em: Emission) -> None:
        if em.kind == "order":
            ctx = SerializationContext(em.topic, MessageField.VALUE)
            payload = serializers[sim.schema_version](em.value, ctx)
        elif em.kind == "click":
            payload = json.dumps(em.value, default=_json_default).encode()
        else:
            payload = em.value
        while True:
            try:
                producer.produce(em.topic, key=em.key.encode(), value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)  # local queue full: back-pressure
        stats.record(em)

    started = time.time()
    last_report = started
    log.info("producing to %s (sessions/sec=%.1f, chaos=%s)", args.bootstrap,
             args.sessions_per_sec, not args.no_chaos)

    while running:
        now = time.time()
        if args.duration and now - started >= args.duration:
            break
        if sim.schema_version == 1 and 0 < args.schema_v2_after <= now - started:
            sim.schema_version = 2
            log.info("schema evolution: switching order events to v2 (adds payment_method)")

        for em in chaos.apply(sim.tick(int(now * 1000)), int(now * 1000)):
            send(em)
        producer.poll(0)

        if now - last_report >= 10:
            last_report = now
            log.info("stats %s | in-flight sim items=%d held-late=%d",
                     json.dumps(stats.as_dict()["sent"]), sim.pending, chaos.held)
        time.sleep(0.05)

    for em in chaos.flush():
        send(em)
    producer.flush(30)

    manifest = merge_manifest(Path(args.manifest), stats.as_dict(), round(time.time() - started, 1))
    Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(args.manifest).write_text(json.dumps(manifest, indent=2))
    log.info("done. manifest written to %s\n%s", args.manifest, json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
