#!/usr/bin/env bash
set -euo pipefail

echo "Docker Compose services:"
docker compose ps

echo
echo "Kafka topics:"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list

echo
echo "Airflow UI: http://localhost:8080"
echo "Spark Master UI: http://localhost:8081"
