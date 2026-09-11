
## Definition of Done

A week is considered complete when:

- the code runs locally without manual fixes;
- the expected output is generated;
- documentation is updated;
- at least one meaningful commit was created;
- known issues are registered in `docs/troubleshooting.md`;
- the weekly log is updated.

## Week 1 — Environment Setup

### Status
Completed.

### What I Built
- Created the initial project structure.
- Configured Docker Compose with Airflow, Kafka, Zookeeper, Spark and Postgres.
- Replaced unavailable Bitnami Spark image with Apache Spark image.
- Created Kafka topic `account-applications`.
- Validated Airflow UI and Spark Master UI.
- Created and validated a basic Airflow healthcheck DAG.

### Problems Faced
- `bitnami/spark:3.5.1` was unavailable on Docker Hub.
- Fixed by switching to `apache/spark:3.5.1-python3`.

### What I Learned
- Difference between host access and internal Docker network access.
- Kafka uses `localhost:9092` from host and `kafka:29092` inside Docker.
- Airflow init container exits after setup, which is expected.
- Spark Master UI confirms cluster health and active workers.

### Next Steps
- Start Week 2: batch ingestion with Airflow.
- Load BAF data into DuckDB by `month`.
- Add data quality checks for row count, fraud distribution and protected attributes.
---

## Week 2 — Batch Ingestion

### Status

In progress. The raw ingestion and its quality gate are functional; monthly
partitioning remains pending until a concrete downstream requirement exists.

### Goals

- Create Airflow DAG.
- Load BAF data by month.
- Write quality checks.

### What I Built

- Added reusable BAF CSV ingestion into `raw.raw_baf_applications` in DuckDB.
- Added staging-table schema validation before replacing the published table.
- Added explicit checks for row count, required fields, labels and month range.
- Updated the manual Airflow DAG to run load and validation in sequence.
- Added focused integration tests using a two-row synthetic CSV.
- Verified a real Airflow run with both tasks completing successfully.

### What I Learned

- The Base dataset has 1,000,000 rows across months 0–7.
- Loading through DuckDB is fast enough for the local portfolio workflow.
- Containerized tests need the project test directory and `PYTHONPATH` mounted explicitly.

### Problems Faced

- The host Python environment does not include DuckDB or pytest.
- The Airflow image entrypoint waits for Postgres unless isolated tests disable that check.
- `airflow dags test` raced with the running scheduler; a normal manual DAG run succeeded.

### Decisions Made

- Keep raw ingestion in DuckDB before starting the streaming layer.
- Use a staging table so invalid input cannot replace a previously valid raw table.
- Keep tests inside the existing Airflow image instead of adding host dependencies.

### Next Steps

- Implement a small Kafka producer that replays a bounded BAF sample.
- Define and test the event schema before implementing Spark Structured Streaming.

## Streaming MVP - 2026-09-10

- Confirmed `data/raw/` as the canonical dataset input; Kaggle is acquisition only.
- Added a versioned flat JSON event contract and shared validation code.
- Added bounded CSV replay (default 100), broker acknowledgements and unit tests.
- Implemented Kafka to Parquet with explicit Spark schema, rejection partition
  and persistent checkpoint, using the existing standalone cluster.
- Real test: 10 BAF rows acknowledged, 10 consumed and 10 distinct valid rows
  read back from Parquet; checkpoint offsets and commit files created.
- Restarted with the same checkpoint: zero new input rows and no duplicate output.
- Resolved the missing Spark Kafka connector with the matching 3.5.1 package.
- Mounted producer under `replay` to avoid shadowing the installed `kafka` library.
- Next milestone: strengthen replay/recovery tests, including invalid events
  and consumer restarts, before adding streaming features.

## Streaming Resilience - 2026-09-10

- Added first-error `validation_errors`, original payload bytes, Kafka coordinates,
  publication/ingestion timestamps and extractable source schema version.
- Expanded unit tests: 14 passed. Missing fields, wrong types, invalid label/month,
  malformed JSON and unsupported version are quarantined without stopping the job.
- Real test `resilience-03a69313cfea`: first batch persisted 10 valid + 6 rejected.
- Killed only the test Spark driver with SIGKILL after that batch committed;
  published 5 additional valid messages while the consumer was down.
- Restarted with the same checkpoint: consumed exactly those 5 pending messages.
- Final assertions: 21 records = 15 valid + 6 rejected; 20 distinct non-null event
  IDs (malformed JSON has none), 21 unique Kafka coordinates, zero observed duplicates.
- Output: `data/streaming/resilience-03a69313cfea/`.
  Checkpoint: `spark/checkpoints/resilience-03a69313cfea/` (retained).
- Reproduce with `python3 scripts/test_streaming_resilience.py`; the test creates
  isolated artifacts and leaves the existing sample output/checkpoint untouched.
- No integration blocker. Earlier Parquet files lack the newly added metadata;
  this evidence covers an abrupt stop between commits, not an in-flight write.
- Next milestone: failure injection during an active microbatch and validation
  using the file sink commit log, including possible orphan files.
