#!/usr/bin/env bash
# Creates the MVP topic set. Idempotent.
set -euo pipefail

BROKER="localhost:9092"

topics=(
  "orders.commands.create.v1:6"
  "orders.commands.cancel.v1:6"
  "orders.events.order-placed.v1:6"
  "orders.events.order-status-changed.v1:6"
  "orders.replies.v1:6"
  "orders.dlq.v1:3"
)

for entry in "${topics[@]}"; do
  name="${entry%%:*}"
  parts="${entry##*:}"
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor 1
done
