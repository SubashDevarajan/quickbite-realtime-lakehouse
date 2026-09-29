"""Schema-evolution guardrail demo.

Asks the Schema Registry whether each candidate schema is compatible with the
latest registered version of ``order_events-value`` under BACKWARD
compatibility (new readers can read old data). v2 (adds an optional field)
must pass; v3 (renames a field, adds a required one) must be rejected.

In CI for a producer service, this check would run before deploy so that a
breaking change can never reach the topic.

Usage:  python -m scripts.check_schema_compat [--url http://localhost:8081]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

SCHEMAS = Path(__file__).resolve().parent.parent / "schemas"
SUBJECT = "order_events-value"


def check(url: str, schema_file: Path) -> tuple[bool, list[str]]:
    body = json.dumps({"schema": schema_file.read_text()}).encode()
    req = urllib.request.Request(
        f"{url}/compatibility/subjects/{SUBJECT}/versions/latest?verbose=true",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read())
    return bool(result.get("is_compatible")), result.get("messages", [])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    args = p.parse_args()

    with urllib.request.urlopen(f"{args.url}/subjects/{SUBJECT}/versions", timeout=10) as resp:
        print(f"registered versions of {SUBJECT}: {json.loads(resp.read())}")

    expectations = {"order_event_v2.avsc": True, "order_event_v3_breaking.avsc": False}
    ok = True
    for name, expected in expectations.items():
        compatible, messages = check(args.url, SCHEMAS / name)
        verdict = "COMPATIBLE" if compatible else "REJECTED"
        mark = "ok " if compatible == expected else "!! "
        print(f"{mark}{name:32s} -> {verdict}")
        for m in messages[:4]:
            print(f"      {m}")
        ok &= compatible == expected
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
