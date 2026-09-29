#!/usr/bin/env bash
# Create topics explicitly (auto-create is disabled on the broker so a typo in
# a producer can never silently create a new topic).
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  local topic=$1 partitions=$2
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$topic" --partitions "$partitions" --replication-factor 1 \
      --config retention.ms=604800000 \
      --config min.insync.replicas=1
  echo "topic ready: $topic ($partitions partitions)"
}

# Partition count = max consumer parallelism. Order events are keyed by
# order_id, so all events of one order land in one partition, in order.
create order_events 6
create clickstream 6

$KT --bootstrap-server "$BOOTSTRAP" --describe --topic 'order_events|clickstream'
