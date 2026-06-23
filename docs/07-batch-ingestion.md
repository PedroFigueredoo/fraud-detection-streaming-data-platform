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
* creates the target raw table;
* adds `dataset_variant`;
* adds `ingested_at`;
* validates required columns;
* prints a dataset summary;
* prints fraud distribution by month.

## Command

The ingestion can be executed inside the Airflow container:

```bash
docker compose exec airflow-webserver python /opt/airflow/ingestion/load_baf_to_duckdb.py --replace
```

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

* Create an Airflow DAG to execute the ingestion script.
* Add explicit data quality checks.
* Validate row counts and fraud distribution after each ingestion.
* Later, extend the ingestion to support additional BAF variants.
