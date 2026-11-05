
import argparse
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from src.ingestion.quarter_dates import validate_quarter_date


ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data" / "bronze" / "fdic" / "financials"
SILVER = ROOT / "data" / "silver" / "fdic" / "financials"
QUARANTINE = ROOT / "data" / "quarantine" / "fdic"

NUMERIC_FIELDS = [
    "ASSET",
    "DEP",
    "LNLSGR",
    "EQ",
    "NETINC",
    "ROA",
    "ROE",
    "NIMY",
    "NCLNLSR",
    "RBCRWAJ",
]

OUTPUT_FIELDS = [
    "CERT",
    "REPDTE",
    *NUMERIC_FIELDS,
]

REQUIRED_FINANCIAL_FIELDS = {"ASSET", "DEP"}


def find_bronze_run(report_date):
    quarter_dir = BRONZE / report_date

    if not quarter_dir.exists():
        raise FileNotFoundError(
            f"No Bronze data for {report_date}"
        )

    for run_dir in sorted(
        quarter_dir.iterdir(), reverse=True
    ):
        manifest_path = run_dir / "manifest.json"

        if not manifest_path.is_file():
            continue

        with manifest_path.open(encoding="utf-8") as file:
            manifest = json.load(file)

        if (
            manifest.get("status") == "SUCCESS"
            and manifest.get("report_date") == report_date
        ):
            return run_dir, manifest

    raise FileNotFoundError(
        f"No successful Bronze run for {report_date}"
    )


def normalize_number(value, field):
    if value is None or value == "":
        if field in REQUIRED_FINANCIAL_FIELDS:
            raise ValueError(f"MISSING_{field}")
        return None

    if isinstance(value, bool):
        raise ValueError(f"INVALID_{field}")

    try:
        number = Decimal(str(value))

        if not number.is_finite():
            raise ValueError(f"INVALID_{field}")

        if field in {"ASSET", "DEP"} and number < 0:
            raise ValueError(f"NEGATIVE_{field}")

        return str(number)

    except (InvalidOperation, TypeError):
        raise ValueError(f"INVALID_{field}") from None


def clean_record(record, expected_date):
    if not isinstance(record, dict):
        raise ValueError("INVALID_RECORD_STRUCTURE")

    cert = record.get("CERT")

    if (
        isinstance(cert, bool)
        or not isinstance(cert, int)
        or cert <= 0
    ):
        raise ValueError("INVALID_CERT")

    raw_date = str(record.get("REPDTE", ""))

    if raw_date != expected_date:
        raise ValueError("WRONG_REPORT_DATE")

    parsed_date = datetime.strptime(
        raw_date, "%Y%m%d"
    ).date()

    cleaned = {
        "CERT": cert,
        "REPDTE": parsed_date.isoformat(),
    }

    for field in NUMERIC_FIELDS:
        cleaned[field] = normalize_number(
            record.get(field), field
        )

    return cleaned


def transform_quarter(report_date):
    validate_quarter_date(report_date)

    run_dir, manifest = find_bronze_run(report_date)

    expected_count = manifest["collected_records"]
    expected_pages = manifest["pages"]

    page_files = sorted(
        run_dir.glob("page_*.json")
    )

    if len(page_files) != expected_pages:
        raise RuntimeError(
            "Bronze page count does not match manifest"
        )

    accepted = []
    rejected = []
    seen_keys = set()
    source_count = 0

    for page_path in page_files:
        with page_path.open(encoding="utf-8") as file:
            response = json.load(file)

        items = response.get("data")

        if not isinstance(items, list):
            raise ValueError(
                f"Invalid page structure: {page_path}"
            )

        for item in items:
            source_count += 1

            record = (
                item.get("data")
                if isinstance(item, dict)
                else None
            )

            try:
                cleaned = clean_record(
                    record, report_date
                )

                key = (
                    cleaned["CERT"],
                    cleaned["REPDTE"],
                )

                if key in seen_keys:
                    raise ValueError(
                        "DUPLICATE_BANK_QUARTER"
                    )

                seen_keys.add(key)
                accepted.append(cleaned)

            except (ValueError, TypeError) as error:
                rejected.append({
                    "source_page": page_path.name,
                    "rejection_reason": str(error),
                    "raw_record": record,
                })

    if source_count != expected_count:
        raise RuntimeError(
            f"Bronze reconciliation failed: "
            f"{source_count} != {expected_count}"
        )

    silver_dir = SILVER / report_date
    quarantine_dir = QUARANTINE / report_date

    silver_dir.mkdir(
        parents=True, exist_ok=True
    )
    quarantine_dir.mkdir(
        parents=True, exist_ok=True
    )

    silver_path = silver_dir / "records.csv"
    rejected_path = (
        quarantine_dir / "rejected_records.jsonl"
    )

    with silver_path.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=OUTPUT_FIELDS
        )
        writer.writeheader()
        writer.writerows(accepted)

    with rejected_path.open(
        "w", encoding="utf-8"
    ) as file:
        for item in rejected:
            file.write(
                json.dumps(item) + "\n"
            )

    report = {
        "report_date": report_date,
        "source_run_id": manifest["run_id"],
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_records": source_count,
        "accepted_records": len(accepted),
        "rejected_records": len(rejected),
        "pass_rate": round(
            len(accepted) / source_count * 100, 2
        ) if source_count else 0,
        "status": "SUCCESS",
    }

    with (
        silver_dir / "quality_report.json"
    ).open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print("\nSilver transformation complete!")
    print(json.dumps(report, indent=2))

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Transform FDIC Bronze data to Silver"
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Reporting quarter YYYYMMDD"
    )

    args = parser.parse_args()
    transform_quarter(args.date)


if __name__ == "__main__":
    main()
