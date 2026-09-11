"""Bounded replay of canonical BAF Base.csv into Kafka."""
import argparse
import csv
import logging
import os
import time
from itertools import islice
from pathlib import Path
from uuid import uuid4

from event_contract import load_contract, serialize_event


def create_event(row: dict, row_number: int, replay_id: str) -> dict:
    return dict(event_id=replay_id + ":" + str(row_number), schema_version=1,
                replay_id=replay_id, source_row=row_number,
                replayed_at_ms=int(time.time() * 1000), dataset_variant="base",
                month=int(row["month"]), customer_age=int(row["customer_age"]),
                employment_status=row["employment_status"], income=float(row["income"]),
                fraud_bool=int(row["fraud_bool"]))


def replay(source, limit: int, delay: float, replay_id: str, schema: dict, producer, topic: str) -> int:
    if limit <= 0 or delay < 0:
        raise ValueError("limit must be positive and interval nonnegative")
    count = 0
    with Path(source).open(newline="") as handle:
        for number, row in enumerate(islice(csv.DictReader(handle), limit), 1):
            event = create_event(row, number, replay_id)
            payload = serialize_event(event, schema)
            producer.send(topic, key=event["event_id"].encode(), value=payload).get(timeout=30)
            count += 1
            if delay:
                time.sleep(delay)
    logging.info("Acknowledged events=%d replay_id=%s", count, replay_id)
    return count


def main():
    from kafka import KafkaProducer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="/opt/airflow/data/raw/Base.csv")
    parser.add_argument("--schema", default="/opt/airflow/replay/schemas/account_application_v1.json")
    parser.add_argument("--bootstrap-server", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default="account-applications")
    parser.add_argument("--limit", type=int, default=int(os.getenv("REPLAY_LIMIT", "100")))
    parser.add_argument("--interval", type=float, default=0)
    parser.add_argument("--replay-id", default=str(uuid4()))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    schema = load_contract(args.schema)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_server, acks="all", retries=3)
    try:
        replay(args.source, args.limit, args.interval, args.replay_id, schema, producer, args.topic)
    except Exception:
        logging.exception("Replay failed; some earlier events may already be acknowledged")
        raise
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
