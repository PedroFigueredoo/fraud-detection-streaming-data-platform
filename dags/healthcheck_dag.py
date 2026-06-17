from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="healthcheck_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcheck"],
)
def healthcheck_dag() -> None:
    @task
    def print_message() -> None:
        print("Fraud detection platform environment is running.")

    print_message()


healthcheck_dag()
