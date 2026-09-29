from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

# Spark converts naive Python datetimes using the process timezone. Pin
# everything to UTC so tests behave identically on a laptop in IST and in CI.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):  # not available on Windows
    time.tzset()

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with Delta + Avro. Only created if a Spark test runs."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from pipeline.spark_session import build_spark

    session = build_spark("tests", master="local[2]", shuffle_partitions=2)
    yield session
    session.stop()


@pytest.fixture
def lake_paths(tmp_path):
    from pipeline.config import Paths

    return Paths(str(tmp_path))
