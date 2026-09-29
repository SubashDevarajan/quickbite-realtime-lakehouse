"""SparkSession factory with Delta Lake, Kafka and Avro support."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.3"
SCALA_VERSION = "2.12"
EXTRA_PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}",
]


def build_spark(app_name: str, *, master: str | None = None, shuffle_partitions: int | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Small local cluster: default 200 shuffle partitions would create
        # hundreds of tiny tasks and tiny files per micro-batch.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or os.getenv("SHUFFLE_PARTITIONS", "4")))
        # RocksDB keeps large streaming state off the JVM heap.
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    ivy = os.getenv("SPARK_IVY_DIR")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    spark = configure_spark_with_delta_pip(builder, extra_packages=EXTRA_PACKAGES).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
