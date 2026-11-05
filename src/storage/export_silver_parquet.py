
import argparse
import csv
import json
from datetime import date
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.quarter_dates import validate_quarter_date


ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data" / "silver" / "fdic" / "financials"

NUMERIC_FIELDS = [
    "ASSET", "DEP", "LNLSGR", "EQ", "NETINC",
    "ROA", "ROE", "NIMY", "NCLNLSR", "RBCRWAJ"
]

DECIMAL_TYPE = pa.decimal128(30, 10)
DECIMAL_UNIT = Decimal("0.0000000001")

SCHEMA = pa.schema([
    pa.field("CERT", pa.int64(), nullable=False),
    pa.field("REPDTE", pa.date32(), nullable=False),
    *[
        pa.field(field, DECIMAL_TYPE)
        for field in NUMERIC_FIELDS
    ],
])


def parse_decimal(value):
    if value is None or value.strip() == "":
        return None

    number = Decimal(value)

    if not number.is_finite():
        raise ValueError("Invalid financial number")

    return number.quantize(
        DECIMAL_UNIT,
        rounding=ROUND_HALF_EVEN
    )


def export_parquet(report_date):
    validate_quarter_date(report_date)

    quarter_dir = SILVER / report_date

    csv_path = quarter_dir / "records.csv"
    report_path = quarter_dir / "quality_report.json"

    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    if not report_path.is_file():
        raise FileNotFoundError(report_path)

    with report_path.open(encoding="utf-8") as file:
        report = json.load(file)

    if (
        report.get("status") != "SUCCESS"
        or report.get("report_date") != report_date
        or report.get("rejected_records") != 0
    ):
        raise RuntimeError(
            "Silver quality gate has not passed"
        )

    rows = []
    seen = set()

    with csv_path.open(
        newline="", encoding="utf-8"
    ) as file:
        reader = csv.DictReader(file)

        required_fields = set(SCHEMA.names)

        if not required_fields.issubset(
            set(reader.fieldnames or [])
        ):
            raise ValueError(
                "Silver CSV is missing required columns"
            )

        for row in reader:
            cert = int(row["CERT"])
            report_day = date.fromisoformat(
                row["REPDTE"]
            )

            if cert <= 0:
                raise ValueError("Invalid bank CERT")

            if report_day.strftime("%Y%m%d") != report_date:
                raise ValueError(
                    "Unexpected reporting date"
                )

            key = (cert, report_day)

            if key in seen:
                raise ValueError(
                    f"Duplicate bank-quarter: {key}"
                )

            seen.add(key)

            cleaned = {
                "CERT": cert,
                "REPDTE": report_day,
            }

            for field in NUMERIC_FIELDS:
                cleaned[field] = parse_decimal(
                    row[field]
                )

            rows.append(cleaned)

    expected = report["accepted_records"]

    if not rows or len(rows) != expected:
        raise RuntimeError(
            f"Record count mismatch: "
            f"{len(rows)} != {expected}"
        )

    table = pa.Table.from_pylist(
        rows, schema=SCHEMA
    )

    output_dir = (
        SILVER / "parquet"
        / f"report_date={report_date}"
    )
    output_dir.mkdir(
        parents=True, exist_ok=True
    )

    output_path = output_dir / "records.parquet"

    pq.write_table(
        table,
        output_path,
        compression="snappy"
    )

    # Check the generated file can be read.
    metadata = pq.read_metadata(output_path)

    if metadata.num_rows != len(rows):
        raise RuntimeError(
            "Parquet verification failed"
        )

    print("\nParquet export successful!")
    print(f"Records: {metadata.num_rows}")
    print(f"Columns: {len(table.schema)}")
    print(f"Output: {output_path}")

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Export validated FDIC Silver to Parquet"
    )

    parser.add_argument(
        "--date",
        required=True
    )

    args = parser.parse_args()

    export_parquet(args.date)


if __name__ == "__main__":
    main()
