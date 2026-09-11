"""Small real-Kafka fixture and Parquet assertions; invoked by the host runner."""
import argparse
import csv
import json
from pathlib import Path

from producer_account_applications import create_event


def publish(topic, phase):
    from kafka import KafkaProducer
    with open('/opt/airflow/data/raw/Base.csv') as handle:
        row = next(csv.DictReader(handle))
    numbers = range(1, 11) if phase == 'first' else range(17, 22)
    payloads = [json.dumps(create_event(row, n, topic)).encode() for n in numbers]
    if phase == 'first':
        for n, field, value in [(11, 'month', None), (12, 'customer_age', 'bad'),
                                (13, 'fraud_bool', 2), (14, 'month', 8),
                                (15, 'schema_version', 2)]:
            event = create_event(row, n, topic)
            if value is None:
                del event[field]
            else:
                event[field] = value
            payloads.append(json.dumps(event).encode())
        payloads.append(b'{broken')
    producer = KafkaProducer(bootstrap_servers='kafka:29092', acks='all')
    try:
        # Intentionally bypass production validation to exercise consumer quarantine.
        for payload in payloads:
            producer.send(topic, value=payload).get(timeout=30)
    finally:
        producer.close()
    print(json.dumps({'acknowledged': len(payloads), 'phase': phase}))


def check(topic, expected):
    import pyarrow.dataset as ds
    root = Path('/opt/airflow/data/streaming') / topic
    rows = ds.dataset(root, format='parquet', partitioning='hive').to_table().to_pylist()
    assert len(rows) == expected, (len(rows), expected)
    rejects = [r for r in rows if r['is_valid'] == 'false']
    assert len(rejects) == 6
    assert {r['validation_errors'][0] for r in rejects} == {
        'Missing fields: month', 'Invalid type: customer_age', 'Invalid enum: fraud_bool',
        'Out of range: month', 'Invalid enum: schema_version', 'Malformed JSON'}
    for r in rows:
        assert r['topic'] == topic
        assert r['kafka_timestamp'] and r['ingested_at']
        assert r['raw_payload'].decode() == r['raw_json']
        if r['is_valid'] == 'true':
            assert not r['validation_errors']
    assert any(r['raw_payload'] == b'{broken' for r in rejects)
    assert any(r['source_schema_version'] == '2' for r in rejects)
    coordinates = {(r['topic'], r['partition'], r['offset']) for r in rows}
    ids = [r['event_id'] for r in rows if r['event_id'] is not None]
    assert len(coordinates) == expected
    assert len(ids) == len(set(ids)) == expected - 1
    assert len({r['event_id'] for r in rows if r['is_valid'] == 'true'}) == expected - 6
    print(json.dumps({'rows': len(rows), 'valid': expected - 6, 'rejected': 6,
                      'distinct_nonnull_event_ids': len(set(ids)),
                      'distinct_kafka_coordinates': len(coordinates)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['publish', 'check'])
    parser.add_argument('topic')
    parser.add_argument('value')
    args = parser.parse_args()
    if args.action == 'publish':
        publish(args.topic, args.value)
    else:
        check(args.topic, int(args.value))
