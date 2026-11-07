
import argparse
import json
import os
import re
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


REGION = "ca-central-1"
BUCKET = "dataops-sentinel-lake-ikana-2026"

SILVER_PREFIX = "silver/fdic/financials/named/"

CURRENT_MANIFEST = "dashboard/gold/latest.json"
HISTORY_MANIFEST = "dashboard/history/latest.json"

PARTITION_PATTERN = re.compile(
    r"^silver/fdic/financials/named/"
    r"report_date=(\d{8})/records\.parquet$"
)


def find_latest_silver_quarter(s3):
    """Identify the newest named Silver Parquet partition."""

    paginator = s3.get_paginator("list_objects_v2")

    dates = set()

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=SILVER_PREFIX,
    ):
        for obj in page.get("Contents", []):
            match = PARTITION_PATTERN.fullmatch(
                obj["Key"]
            )

            if match:
                dates.add(match.group(1))

    if not dates:
        raise ValueError(
            "No named Silver Parquet partitions found"
        )

    return max(dates)


def read_manifest(s3, key, date_field):
    """Return the published reporting date from S3."""

    try:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )
    except ClientError as error:
        code = error.response["Error"]["Code"]

        if code in ("NoSuchKey", "404"):
            return None

        raise

    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    date = manifest.get(date_field)

    if not isinstance(date, str) or not re.fullmatch(
        r"\d{8}", date
    ):
        raise ValueError(
            f"Invalid date in manifest: {key}"
        )

    return date


def determine_refresh(
    silver_date,
    current_date,
    history_date,
):
    """Decide whether published snapshots lag Silver."""

    if current_date and current_date > silver_date:
        raise ValueError(
            "Current snapshot is newer than Silver"
        )

    if history_date and history_date > silver_date:
        raise ValueError(
            "Historical snapshot is newer than Silver"
        )

    return {
        "silver_date": silver_date,
        "current_snapshot_date": current_date,
        "history_snapshot_date": history_date,
        "refresh_current": (
            current_date is None
            or current_date < silver_date
        ),
        "refresh_history": (
            history_date is None
            or history_date < silver_date
        ),
    }


def check_refresh():
    """Compare source partitions with published snapshots."""

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    silver_date = find_latest_silver_quarter(s3)

    current_date = read_manifest(
        s3,
        CURRENT_MANIFEST,
        "report_date",
    )

    history_date = read_manifest(
        s3,
        HISTORY_MANIFEST,
        "latest_report_date",
    )

    result = determine_refresh(
        silver_date,
        current_date,
        history_date,
    )

    print("\nDATAOPS SENTINEL — REFRESH CHECK")
    print("=" * 45)

    for key, value in result.items():
        print(f"{key}: {value}")

    needs_refresh = (
        result["refresh_current"]
        or result["refresh_history"]
    )

    print(
        "Status: "
        + (
            "NEW DATA DETECTED"
            if needs_refresh
            else "SNAPSHOTS CURRENT"
        )
    )

    # Make the result available to GitHub Actions.
    output_path = os.environ.get("GITHUB_OUTPUT")

    if output_path:
        with Path(output_path).open(
            "a", encoding="utf-8"
        ) as file:
            file.write(
                f"needs_refresh={str(needs_refresh).lower()}\n"
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
    )

    mode.add_argument(
        "--check",
        action="store_true",
    )

    args = parser.parse_args()

    if args.plan:
        print("DATAOPS SENTINEL REFRESH PLAN")
        print(f"Bucket: {BUCKET}")
        print(f"Silver prefix: {SILVER_PREFIX}")
        print(f"Current manifest: {CURRENT_MANIFEST}")
        print(f"History manifest: {HISTORY_MANIFEST}")
        print("No AWS requests made.")
        return

    check_refresh()


if __name__ == "__main__":
    main()
