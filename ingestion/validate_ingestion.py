from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from ingestion.load_baf_to_duckdb import DEFAULT_DATABASE, DEFAULT_TABLE, parse_table_name


REQUIRED_COLUMNS = {
    "customer_age",
    "dataset_variant",
    "employment_status",
    "fraud_bool",
    "income",
    "ingested_at",
    "month",
}
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class QualityReport:
    total_rows: int
    total_months: int
    min_month: int
    max_month: int
    fraud_rows: int


def validate_raw_table(
    database: Path,
    table: str = DEFAULT_TABLE,
    expected_rows: int | None = None,
    expected_min_month: int | None = None,
    expected_max_month: int | None = None,
) -> QualityReport:
    database = database.resolve()
    schema, table_name = parse_table_name(table)
    if not database.is_file():
        raise FileNotFoundError(f"Database not found: {database}")

    connection = duckdb.connect(str(database), read_only=True)
    try:
        columns = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table_name],
            ).fetchall()
        }
        if not columns:
            raise ValueError(f"Table not found: {table}")

        missing_columns = REQUIRED_COLUMNS - columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

        row = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT month) AS total_months,
                MIN(month) AS min_month,
                MAX(month) AS max_month,
                SUM(CAST(fraud_bool AS INTEGER)) AS fraud_rows,
                COUNT(*) FILTER (
                    WHERE fraud_bool IS NULL
                       OR month IS NULL
                       OR customer_age IS NULL
                       OR employment_status IS NULL
                       OR income IS NULL
                       OR dataset_variant IS NULL
                       OR ingested_at IS NULL
                ) AS invalid_null_rows,
                COUNT(*) FILTER (WHERE fraud_bool NOT IN (0, 1)) AS invalid_labels
            FROM {table}
            """
        ).fetchone()
    finally:
        connection.close()

    report = QualityReport(*row[:5])
    failures: list[str] = []
    if report.total_rows == 0:
        failures.append("table is empty")
    if row[5] > 0:
        failures.append(f"{row[5]} rows contain nulls in required fields")
    if row[6] > 0:
        failures.append(f"{row[6]} rows contain invalid fraud labels")
    if expected_rows is not None and report.total_rows != expected_rows:
        failures.append(f"expected {expected_rows} rows, found {report.total_rows}")
    if expected_min_month is not None and report.min_month != expected_min_month:
        failures.append(
            f"expected minimum month {expected_min_month}, found {report.min_month}"
        )
    if expected_max_month is not None and report.max_month != expected_max_month:
        failures.append(
            f"expected maximum month {expected_max_month}, found {report.max_month}"
        )
    if failures:
        raise ValueError("Data quality validation failed: " + "; ".join(failures))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the ingested BAF raw table.")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--expected-min-month", type=int)
    parser.add_argument("--expected-max-month", type=int)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args()
    report = validate_raw_table(
        database=args.database,
        table=args.table,
        expected_rows=args.expected_rows,
        expected_min_month=args.expected_min_month,
        expected_max_month=args.expected_max_month,
    )
    LOGGER.info("Data quality validation passed for %s", args.table)
    LOGGER.info(
        "Rows: %s | months: %s (%s to %s) | fraud rows: %s",
        f"{report.total_rows:,}",
        report.total_months,
        report.min_month,
        report.max_month,
        f"{report.fraud_rows:,}",
    )


if __name__ == "__main__":
    main()
