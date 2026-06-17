#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic account-applications \
  --partitions 3 \
  --replication-factor 1
