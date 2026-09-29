"""Interactive PySpark shell over the lakehouse.

    make sql
    >>> q("SELECT zone, sum(gmv) FROM zone_metrics_1m GROUP BY zone")
    >>> q("DESCRIBE HISTORY delta.`/data/lake/silver/orders_current`")
    >>> q("SELECT * FROM order_events VERSION AS OF 0 LIMIT 5")   -- time travel
"""

from delta.tables import DeltaTable

from pipeline.config import SETTINGS
from pipeline.spark_session import build_spark

spark = build_spark("lakehouse-shell")
_p = SETTINGS.paths
VIEWS = {
    "events_raw": _p.bronze_events,
    "order_events": _p.silver_order_events,
    "orders_current": _p.silver_orders_current,
    "clickstream": _p.silver_clickstream,
    "dead_letter": _p.dead_letter,
    "zone_metrics_1m": _p.gold_zone_metrics,
    "sla_alerts": _p.gold_sla_alerts,
    "checkout_conversion": _p.gold_checkout_conversion,
    "user_sessions": _p.gold_sessions,
}
for _name, _path in VIEWS.items():
    if DeltaTable.isDeltaTable(spark, _path):
        spark.read.format("delta").load(_path).createOrReplaceTempView(_name)


def q(sql: str, n: int = 20) -> None:
    """Run SQL and print the result."""
    spark.sql(sql).show(n, truncate=False)


print("views:", ", ".join(VIEWS))
print('try: q("SELECT status, count(*) FROM orders_current GROUP BY status")')
