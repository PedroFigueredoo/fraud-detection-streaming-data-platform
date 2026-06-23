
from __future__ import annotations



import argparse

from pathlib import Path





import duckdb





PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SOURCE_FILE = PROJECT_ROOT / "data" / "raw" / "Base.csv"

DEFAULT_DATABASE = PROJECT_ROOT / "warehouse" / "fraud.duckdb"

DEFAULT_TABLE = "raw.raw_baf_applications"





def escape_sql_path(path: Path) -> str:

    return str(path).replace("'", "''")





def main() -> None:

    parser = argparse.ArgumentParser(

        description="Load the BAF dataset into a local DuckDB warehouse."

    )

    parser.add_argument(

        "--source-file",

        type=Path,

        default=DEFAULT_SOURCE_FILE,

        help="Path to the BAF CSV file.",

    )

    parser.add_argument(

        "--database",

        type=Path,

        default=DEFAULT_DATABASE,

        help="Path to the DuckDB database file.",

    )

    parser.add_argument(

        "--table",

        default=DEFAULT_TABLE,

        help="Target table name.",

    )

    parser.add_argument(

        "--variant-name",

        default="base",

        help="Dataset variant name to store in the raw table.",

    )

    parser.add_argument(

        "--replace",

        action="store_true",

        help="Replace target table if it already exists.",

    )



    args = parser.parse_args()



    source_file = args.source_file.resolve()

    database = args.database.resolve()

    table = args.table

    variant_name = args.variant_name



    if not source_file.exists():

        raise FileNotFoundError(f"Source file not found: {source_file}")



    database.parent.mkdir(parents=True, exist_ok=True)



    conn = duckdb.connect(str(database))



    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")



    if args.replace:

        conn.execute(f"DROP TABLE IF EXISTS {table}")



    source_file_sql = escape_sql_path(source_file)

    variant_name_sql = variant_name.replace("'", "''")



    conn.execute(

        f"""

        CREATE TABLE {table} AS

        SELECT

            *,

            '{variant_name_sql}'::VARCHAR AS dataset_variant,

            CURRENT_TIMESTAMP AS ingested_at

        FROM read_csv_auto(

            '{source_file_sql}',

            header = true,

            sample_size = -1

        )

        """

    )



    columns = {

        row[1]

        for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()

    }



    required_columns = {"fraud_bool", "month"}

    missing_columns = required_columns - columns



    if missing_columns:

        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")



    summary = conn.execute(

        f"""

        SELECT

            COUNT(*) AS total_rows,

            COUNT(DISTINCT month) AS total_months,

            MIN(month) AS min_month,

            MAX(month) AS max_month,

            SUM(CAST(fraud_bool AS INTEGER)) AS fraud_rows,

            ROUND(AVG(CAST(fraud_bool AS DOUBLE)) * 100, 4) AS fraud_rate_pct

        FROM {table}

        """

    ).fetchone()



    print("\n[OK] BAF dataset loaded into DuckDB")

    print(f"[OK] Source file: {source_file}")

    print(f"[OK] Database: {database}")

    print(f"[OK] Table: {table}")



    print("\nDataset summary")

    print(f"- Total rows: {summary[0]:,}")

    print(f"- Total months: {summary[1]}")

    print(f"- Month range: {summary[2]} to {summary[3]}")

    print(f"- Fraud rows: {summary[4]:,}")

    print(f"- Fraud rate: {summary[5]}%")



    print("\nFraud rate by month")

    rows = conn.execute(

        f"""

        SELECT

            month,

            COUNT(*) AS total_rows,

            SUM(CAST(fraud_bool AS INTEGER)) AS fraud_rows,

            ROUND(AVG(CAST(fraud_bool AS DOUBLE)) * 100, 4) AS fraud_rate_pct

        FROM {table}

        GROUP BY month

        ORDER BY month

        """

    ).fetchall()



    for row in rows:

        print(

            f"- month={row[0]} | rows={row[1]:,} | "

            f"fraud_rows={row[2]:,} | fraud_rate={row[3]}%"

        )



    conn.close()





if __name__ == "__main__":

    main()

