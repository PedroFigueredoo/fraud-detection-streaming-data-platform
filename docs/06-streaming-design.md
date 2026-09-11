# Streaming Design

## Goal

Simulate real-time bank account applications using Kafka and Spark Structured Streaming.

## Kafka Topic

| Topic | Description |
|---|---|
| `account-applications` | Raw account application events |

## Event Schema

Implemented contract: `kafka/schemas/account_application_v1.json` (JSON Schema,
version 1). The example below was the original draft; v1 uses integer
`customer_age` and `replayed_at_ms` instead of an invented business timestamp.
`event_id` identifies a replay row, not a bank application in the source.
The shared `processing/streaming/event_contract.py` validates the flat v1
contract in both producer and Spark; it is not a general JSON Schema engine.

```json
{
  "application_id": "string",
  "event_timestamp": "string",
  "month": 0,
  "customer_age": "string",
  "employment_status": "string",
  "income": 0,
  "fraud_bool": 0
}
````

## Spark Streaming Responsibilities

- consume events from Kafka;
    
- parse JSON schema;
    
- validate required fields;
    
- calculate streaming features;
    
- send invalid records to dead-letter queue;
    
- write scored applications to DuckDB or local parquet.
    

## Streaming Features

The features below remain future work. The current MVP only validates and
persists replay events, without scoring or business feature engineering.

- application volume by time window;
    
- volume by customer age group;
    
- suspicious application rate by income range;
    
- application velocity by device/session features;
    
- score distribution by window.

## Running the MVP

Start the existing Compose environment, then:

```bash
make test-streaming
make produce-sample ARGS='--limit 10 --interval 0'
make stream ARGS='--available-now'
```

`make produce-sample` defaults to 100 rows of canonical `data/raw/Base.csv`;
`--limit` or `REPLAY_LIMIT` controls the bound. The producer reads CSV rows
incrementally, validates before sending and waits for broker acknowledgements.
Use `--bootstrap-server` (or `KAFKA_BOOTSTRAP_SERVERS`) and `--replay-id`
when required. A new run has a new replay ID; replays are not business deduplication.

`make stream` runs continuously on `spark://spark-master:7077`.
The matching Spark 3.5.1 Kafka connector is downloaded on first use into the
container Ivy cache; network access is needed again after container recreation.
The shared validator is distributed to workers through `--py-files`.

Output: `data/streaming/account_applications/is_valid=true/` (valid records)
and `is_valid=false/` (rejected payloads). Kafka partition/offset and raw JSON
are retained. Checkpoint: `spark/checkpoints/account_applications/`.
These artifacts are ignored by Git. Master and worker share the same local mounts.
The first run starts at earliest retained offsets; resumed runs use the checkpoint.
Keep output and checkpoint together and run only one writer for these paths.
`--output` and `--checkpoint` allow separate development runs.

v1 includes only month, age, employment status, income and fraud label from BAF.
Replay metadata: schema version, replay ID, source row (1-based excluding header),
event ID, dataset variant and replay wall-clock milliseconds. `fraud_bool` is a
historical label, not a score. The v1 Base contract accepts months 0-7.

## Rejection and Recovery

Invalid messages remain in `is_valid=false` with `validation_errors`, an array
containing the first structural failure. Valid messages have an empty array.
The consumer preserves `raw_payload` (original bytes), `raw_json`, Kafka `topic`,
`partition`, `offset`, `kafka_timestamp`, and processing `ingested_at`.
`source_schema_version` preserves the JSON version when extractable, independently
of typed parsing. Malformed JSON may have no event ID or version; Kafka coordinates
remain its audit identity. No new business validation rules were added.

Files produced by earlier code do not contain the new audit columns. Read mixed
historical Parquet with schema merging when needed; old metadata cannot be backfilled
from the output alone. This test uses separate output to avoid mixing schemas.

Run the bounded real-cluster test from the repository root:

```bash
python3 scripts/test_streaming_resilience.py
```

The runner requires the existing services and creates one unique test topic with
three partitions, a Parquet directory under `data/streaming/resilience-*` and a
checkpoint under `spark/checkpoints/resilience-*`. It injects malformed fixtures
directly through Kafka: the normal producer correctly refuses these messages.
It verifies 10 valid and 6 rejected records, waits for a committed batch, then sends
SIGKILL only to the Spark driver whose command contains this test's checkpoint.
It publishes 5 valid records while the consumer is down and restarts using the
same checkpoint. Assertions cover 21 final records, all rejection reasons,
payloads, audit metadata, distinct IDs and distinct Kafka coordinates.
Artifacts and logs are retained; nothing is deleted by the runner.

This demonstrates recovery after an abrupt stop between committed microbatches,
not during an in-flight write. It does not guarantee global exactly-once or
deduplicate producer replays. Keep one active writer per output/checkpoint pair.
    
