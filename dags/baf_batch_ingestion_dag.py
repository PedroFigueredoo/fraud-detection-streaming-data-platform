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

    load_base_dataset
