from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_FILE = PROJECT_ROOT / "data" / "raw" / "Base.csv"
DEFAULT_DATABASE = PROJECT_ROOT / "warehouse" / "fraud.duckdb"
DEFAULT_TABLE = "raw.raw_baf_applications"
REQUIRED_SOURCE_COLUMNS = {
    "customer_age",
    "employment_status",
    "fraud_bool",
    "income",
    "month",
}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionSummary:
    total_rows: int
    total_months: int
    min_month: int
    max_month: int
    fraud_rows: int
    fraud_rate_pct: float


def parse_table_name(table: str) -> tuple[str, str]:
    parts = table.split(".")
    if len(parts) != 2 or any(not IDENTIFIER_PATTERN.fullmatch(part) for part in parts):
        raise ValueError("Table must use a valid schema.table name")
    return parts[0], parts[1]


def escape_sql_literal(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _table_columns(connection: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        row[1]
        for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    }


def ingest_baf_dataset(
    source_file: Path,
    database: Path,
    table: str = DEFAULT_TABLE,
    variant_name: str = "base",
    replace: bool = False,
) -> IngestionSummary:
    source_file = source_file.resolve()
    database = database.resolve()
    schema, table_name = parse_table_name(table)

    if not source_file.is_file():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    database.parent.mkdir(parents=True, exist_ok=True)
    staging_table = f"{schema}._{table_name}_staging_{uuid4().hex}"
    source_literal = escape_sql_literal(source_file)
    variant_literal = escape_sql_literal(variant_name)

    connection = duckdb.connect(str(database))
    try:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        target_exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table_name],
        ).fetchone()[0]
        if target_exists and not replace:
            raise ValueError(f"Target table already exists: {table}")

        connection.execute(
            f"""
            CREATE TABLE {staging_table} AS
            SELECT
                *,
                '{variant_literal}'::VARCHAR AS dataset_variant,
                CURRENT_TIMESTAMP AS ingested_at
            FROM read_csv_auto(
                '{source_literal}',
                header = true,
                sample_size = -1
            )
            """
        )

        missing_columns = REQUIRED_SOURCE_COLUMNS - _table_columns(
            connection, staging_table
        )
        if missing_columns:
            raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

        connection.execute("BEGIN TRANSACTION")
        if replace:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
        connection.execute(f"ALTER TABLE {staging_table} RENAME TO {table_name}")
        connection.execute("COMMIT")

        summary_row = connection.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT month),
                MIN(month),
                MAX(month),
                SUM(CAST(fraud_bool AS INTEGER)),
                ROUND(AVG(CAST(fraud_bool AS DOUBLE)) * 100, 4)
            FROM {table}
            """
        ).fetchone()
        return IngestionSummary(*summary_row)
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except duckdb.TransactionException:
            pass
        connection.execute(f"DROP TABLE IF EXISTS {staging_table}")
        raise
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load the BAF dataset into a local DuckDB warehouse."
    )
    parser.add_argument("--source-file", type=Path, default=DEFAULT_SOURCE_FILE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--variant-name", default="base")
    parser.add_argument("--replace", action="store_true")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args()
    summary = ingest_baf_dataset(
        source_file=args.source_file,
        database=args.database,
        table=args.table,
        variant_name=args.variant_name,
        replace=args.replace,
    )

    LOGGER.info("BAF dataset loaded into %s", args.table)
    LOGGER.info("Rows: %s", f"{summary.total_rows:,}")
    LOGGER.info(
        "Months: %s (%s to %s)",
        summary.total_months,
        summary.min_month,
        summary.max_month,
    )
    LOGGER.info(
        "Fraud rows: %s (%.4f%%)",
        f"{summary.fraud_rows:,}",
        summary.fraud_rate_pct,
    )


if __name__ == "__main__":
    main()
