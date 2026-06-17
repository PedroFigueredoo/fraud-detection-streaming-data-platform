
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

### Goals

- Create Airflow DAG.
- Load BAF data by month.
- Write quality checks.

### What I Built

- 

### What I Learned

- 

### Problems Faced

- 

### Decisions Made

- 

### Next Steps

- 

