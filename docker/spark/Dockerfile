# Spark 3.5 + Delta 3.2 + Kafka/Avro connectors, PySpark in local mode.
# One image runs every pipeline job (bronze, silver, gold, maintenance, verify).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_IVY_DIR=/opt/ivy \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements/spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Resolve the Delta / Kafka / Avro jars at build time so containers start
# fast and do not depend on Maven Central being reachable at runtime.
COPY pipeline/__init__.py pipeline/spark_session.py /tmp/warmup/pipeline/
RUN cd /tmp/warmup && python -c "from pipeline.spark_session import build_spark; build_spark('warmup').stop()" \
 && rm -rf /tmp/warmup

COPY pipeline /app/pipeline
COPY scripts /app/scripts
COPY schemas /app/schemas
