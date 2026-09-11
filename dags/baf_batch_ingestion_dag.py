from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="baf_batch_ingestion",
    description="Load the BAF Base dataset into DuckDB raw layer.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["baf", "batch", "duckdb", "fraud"],
) as dag:
    load_base_dataset = BashOperator(
        task_id="load_base_dataset_to_duckdb",
        bash_command=(
            "python /opt/airflow/ingestion/load_baf_to_duckdb.py "
            "--source-file /opt/airflow/data/raw/Base.csv "
            "--database /opt/airflow/warehouse/fraud.duckdb "
            "--table raw.raw_baf_applications "
            "--variant-name base "
            "--replace"
        ),
    )

    validate_base_dataset = BashOperator(
        task_id="validate_base_dataset",
        bash_command=(
            "cd /opt/airflow && "
            "python -m ingestion.validate_ingestion "
            "--database /opt/airflow/warehouse/fraud.duckdb "
            "--table raw.raw_baf_applications "
            "--expected-rows 1000000 "
            "--expected-min-month 0 "
            "--expected-max-month 7"
        ),
    )

    load_base_dataset >> validate_base_dataset
