
import argparse
import calendar
import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import boto3

from src.pipeline.check_dashboard_refresh import (
    BUCKET,
    REGION,
    find_latest_silver_quarter,
)


FDIC_URL = "https://api.fdic.gov/banks/financials"

TIMEOUT_SECONDS = 30


def validate_quarter(date_value):
    """
    Validate YYYYMMDD and ensure the date
    represents a real calendar quarter end.
    """

    date_text = str(date_value)

    if not re.fullmatch(r"\d{8}", date_text):
        raise ValueError(
            f"Invalid FDIC reporting date: {date_text}"
        )

    date = datetime.strptime(
        date_text, "%Y%m%d"
    ).date()

    if date.month not in (3, 6, 9, 12):
        raise ValueError(
            f"Not a quarter-end date: {date_text}"
        )

    last_day = calendar.monthrange(
        date.year, date.month
    )[1]

    if date.day != last_day:
        raise ValueError(
            f"Not a valid quarter end: {date_text}"
        )

    return date_text


def fetch_latest_fdic_quarter():
    """
    Ask the FDIC API for its most recent
    financial reporting quarter.

    This is read-only and requires no AWS access.
    """

    params = urlencode({
        "fields": "CERT,REPDTE",
        "sort_by": "REPDTE",
        "sort_order": "DESC",
        "limit": 1,
        "format": "json",
    })

    url = f"{FDIC_URL}?{params}"

    print("Checking FDIC financial data availability...")

    with urlopen(
        url,
        timeout=TIMEOUT_SECONDS,
    ) as response:
        payload = json.load(response)

    records = payload.get("data", [])

    if not records:
        raise ValueError(
            "FDIC API returned no financial records"
        )

    first = records[0]

    # FDIC normally nests the selected fields
    # inside the record's 'data' object.
    financial_data = first.get("data", first)

    report_date = financial_data.get("REPDTE")

    if report_date is None:
        raise ValueError(
            "FDIC response missing REPDTE"
        )

    return validate_quarter(report_date)


def determine_ingestion(
    fdic_date,
    silver_date,
):
    """Determine whether ingestion is needed."""

    fdic_date = validate_quarter(fdic_date)
    silver_date = validate_quarter(silver_date)

    if fdic_date < silver_date:
        raise ValueError(
            "FDIC latest quarter is older than "
            "our processed Silver data. "
            "Check the API response."
        )

    return {
        "fdic_latest_quarter": fdic_date,
        "silver_latest_quarter": silver_date,
        "ingestion_required": fdic_date > silver_date,
    }


def check_for_new_quarter():
    """Compare public FDIC data with S3 Silver data."""

    fdic_date = fetch_latest_fdic_quarter()

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    silver_date = find_latest_silver_quarter(s3)

    result = determine_ingestion(
        fdic_date,
        silver_date,
    )

    print("\nDATAOPS SENTINEL — FDIC AVAILABILITY")
    print("=" * 45)

    for key, value in result.items():
        print(f"{key}: {value}")

    if result["ingestion_required"]:
        print("Status: NEW FDIC QUARTER DETECTED")
        print("Ingestion is required but not started.")
    else:
        print("Status: FDIC DATA ALREADY PROCESSED")

    output_path = os.environ.get("GITHUB_OUTPUT")

    if output_path:
        with Path(output_path).open(
            "a", encoding="utf-8"
        ) as file:
            file.write(
                "ingestion_required="
                f"{str(result['ingestion_required']).lower()}\n"
            )
            file.write(
                f"fdic_date={fdic_date}\n"
            )
            file.write(
                f"silver_date={silver_date}\n"
            )

    return result


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--plan",
        action="store_true",
        help="Display configuration without network access",
    )

    mode.add_argument(
        "--check",
        action="store_true",
        help="Check the FDIC API and latest S3 quarter",
    )

    args = parser.parse_args()

    if args.plan:
        print("DATAOPS SENTINEL — FDIC DETECTION PLAN")
        print(f"FDIC endpoint: {FDIC_URL}")
        print(f"S3 bucket: {BUCKET}")
        print(f"AWS region: {REGION}")
        print("Mode: READ-ONLY DETECTION")
        print("No network or AWS requests made.")
        return

    check_for_new_quarter()


if __name__ == "__main__":
    main()

