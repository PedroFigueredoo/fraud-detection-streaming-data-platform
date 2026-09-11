# Batch Ingestion

## Goal

The goal of the batch ingestion step is to load the BAF Base dataset from a local CSV file into a local DuckDB warehouse.

This is the first implementation of the raw data layer for the fraud detection platform.

## Input

```text
data/raw/Base.csv
```

## Output

```text
warehouse/fraud.duckdb
```

Target table:

```text
raw.raw_baf_applications
```

## Current Flow

```text
Base.csv
  ↓
Python ingestion script
  ↓
DuckDB database
  ↓
raw.raw_baf_applications
```

## Script

The ingestion logic is implemented in:

```text
ingestion/load_baf_to_duckdb.py
```

The script:

* reads the BAF CSV file;
* creates the `raw` schema if it does not exist;
* loads a staging table and validates its schema before publishing it;
* adds `dataset_variant`;
* adds `ingested_at`;
* validates required columns;
* prints a dataset summary;
* replaces the target table atomically when `--replace` is used.

## Command

The ingestion can be executed inside the Airflow container:

```bash
docker compose exec airflow-webserver python /opt/airflow/ingestion/load_baf_to_duckdb.py --replace
```

Convenience commands:

```bash
make ingest-base
make validate-base
```

The manual Airflow DAG `baf_batch_ingestion` runs the load first and then checks:

* exact row count for the Base dataset;
* required source and metadata columns;
* nulls in required fields;
* valid binary fraud labels;
* expected month range.

## Validation Result

The first successful execution loaded:

| Metric       |     Value |
| ------------ | --------: |
| Total rows   | 1,000,000 |
| Total months |         8 |
| Month range  |    0 to 7 |
| Fraud rows   |    11,029 |
| Fraud rate   |   1.1029% |

## Observations

The generated DuckDB file is local and should not be committed:

```text
warehouse/fraud.duckdb
```

The raw CSV files are also local and ignored by Git:

```text
data/raw/
```

This keeps the repository lightweight and reproducible while avoiding large data files in version control.

## Next Steps

* Partition the batch output by month when a downstream consumer requires it.
* Add an ingestion run audit table before supporting incremental loads.
* Later, extend the ingestion to support additional BAF variants.
