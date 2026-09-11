from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ingestion.load_baf_to_duckdb import ingest_baf_dataset
from ingestion.validate_ingestion import validate_raw_table


VALID_CSV = """fraud_bool,income,customer_age,employment_status,month
0,0.3,40,CB,0
1,0.7,50,CA,1
"""


def test_ingestion_and_quality_validation(tmp_path: Path) -> None:
    source_file = tmp_path / "Base.csv"
    database = tmp_path / "fraud.duckdb"
    source_file.write_text(VALID_CSV, encoding="utf-8")

    summary = ingest_baf_dataset(
        source_file=source_file,
        database=database,
        variant_name="base",
        replace=True,
    )
    report = validate_raw_table(
        database=database,
        expected_rows=2,
        expected_min_month=0,
        expected_max_month=1,
    )

    assert summary.total_rows == 2
    assert summary.fraud_rows == 1
    assert report.total_months == 2

    connection = duckdb.connect(str(database), read_only=True)
    try:
        assert connection.execute(
            "SELECT DISTINCT dataset_variant FROM raw.raw_baf_applications"
        ).fetchone()[0] == "base"
    finally:
        connection.close()


def test_missing_required_column_does_not_publish_target(tmp_path: Path) -> None:
    source_file = tmp_path / "invalid.csv"
    database = tmp_path / "fraud.duckdb"
    source_file.write_text(
        "fraud_bool,income,customer_age,employment_status\n0,0.3,40,CB\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Missing required columns: \['month'\]"):
        ingest_baf_dataset(source_file=source_file, database=database, replace=True)

    connection = duckdb.connect(str(database), read_only=True)
    try:
        table_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = 'raw_baf_applications'
            """
        ).fetchone()[0]
    finally:
        connection.close()

    assert table_count == 0
