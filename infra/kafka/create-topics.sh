#!/usr/bin/env bash
# Создание Kafka-топиков PythonEE (MVP).
# Источник истины: doc/contracts/topics.md §2.
# Идемпотентен: повторный запуск не падает на существующих топиках.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

# Формат: "topic:partitions:retention_ms"
TOPICS=(
  # commands (7d)
  "catalog.commands.create-product.v1:3:604800000"
  "orders.commands.create.v1:6:604800000"
  "orders.commands.ship.v1:6:604800000"
  "delivery.commands.mark-delivered.v1:3:604800000"

  # events (30d)
  "orders.events.order-placed.v1:6:2592000000"
  "orders.events.order-shipped.v1:6:2592000000"
  "orders.events.order-status-changed.v1:6:2592000000"
  "delivery.events.delivery-assigned.v1:3:2592000000"
  "delivery.events.delivery-completed.v1:3:2592000000"

  # DLQ (1 partition, 30d) — для каждого source-топика
  "catalog.commands.create-product.v1.dlq.v1:1:2592000000"
  "orders.commands.create.v1.dlq.v1:1:2592000000"
  "orders.commands.ship.v1.dlq.v1:1:2592000000"
  "delivery.commands.mark-delivered.v1.dlq.v1:1:2592000000"
  "orders.events.order-placed.v1.dlq.v1:1:2592000000"
  "orders.events.order-shipped.v1.dlq.v1:1:2592000000"
  "orders.events.order-status-changed.v1.dlq.v1:1:2592000000"
  "delivery.events.delivery-assigned.v1.dlq.v1:1:2592000000"
  "delivery.events.delivery-completed.v1.dlq.v1:1:2592000000"
)

echo "Creating topics at ${BOOTSTRAP} (replication=${REPLICATION})..."

for entry in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions retention <<< "${entry}"
  echo "  -> ${topic} (partitions=${partitions}, retention.ms=${retention})"
  "${KAFKA_TOPICS_BIN}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION}" \
    --config "retention.ms=${retention}"
done

echo "Done. Topics:"
"${KAFKA_TOPICS_BIN}" --bootstrap-server "${BOOTSTRAP}" --list | sort
