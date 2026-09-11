"""Kafka to local Parquet using the existing standalone Spark cluster."""
import argparse
import json
import logging

from pyspark.sql import SparkSession, functions as F, types as T
from event_contract import load_contract, validation_errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--topic", default="account-applications")
    parser.add_argument("--output", default="/opt/spark/data/streaming/account_applications")
    parser.add_argument("--checkpoint", default="/opt/spark/checkpoints/account_applications")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    contract = load_contract("/opt/spark/replay/schemas/account_application_v1.json")
    types = {"string": T.StringType(), "integer": T.LongType(), "number": T.DoubleType()}
    schema = T.StructType([T.StructField(name, types[rule["type"]], True)
                           for name, rule in contract["properties"].items()])
    spark = SparkSession.builder.appName("baf-account-applications-v1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    errors = F.udf(lambda raw: validation_errors(raw, contract), T.ArrayType(T.StringType()))
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap_server)
           .option("subscribe", args.topic).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 1000).load())
    # Rejected payloads are retained in a separate partition for inspection.
    records = (raw.selectExpr("CAST(value AS STRING) AS raw_json", "value AS raw_payload",
                             "topic", "partition", "offset", "timestamp AS kafka_timestamp")
               .withColumn("ingested_at", F.current_timestamp())
               .withColumn("validation_errors", errors("raw_json"))
               .withColumn("is_valid", F.size("validation_errors") == 0)
               .withColumn("source_schema_version", F.get_json_object("raw_json", "$.schema_version"))
               .withColumn("event", F.from_json("raw_json", schema))
               .select("event.*", "is_valid", "validation_errors", "raw_json", "raw_payload",
                       "source_schema_version", "kafka_timestamp", "ingested_at",
                       "topic", "partition", "offset"))
    writer = (records.writeStream.format("parquet").outputMode("append")
              .partitionBy("is_valid").option("path", args.output)
              .option("checkpointLocation", args.checkpoint))
    query = (writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="5 seconds")).start()
    try:
        query.awaitTermination()
        logging.info("Streaming progress: %s", json.dumps(query.recentProgress))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
