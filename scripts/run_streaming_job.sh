#!/usr/bin/env bash
set -euo pipefail
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --conf spark.driver.host=spark-master --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=3 \
  --conf spark.jars.ivy=/tmp/baf-ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --py-files /opt/spark/processing/streaming/event_contract.py \
  /opt/spark/processing/streaming/spark_streaming_job.py "$@"
