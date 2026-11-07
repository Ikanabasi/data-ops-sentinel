
import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.pipeline.deploy_gold_views import (
    REGION,
    WORKGROUP,
)
from src.pipeline.publish_dashboard_snapshot import (
    BUCKET,
    fetch_rows,
)


PREFIX = "dashboard/history"
LATEST_KEY = f"{PREFIX}/latest.json"

SILVER_TABLE = (
    "dataops_sentinel.fdic_financials_named"
)

GOLD_VIEW = (
    "dataops_sentinel.gold_unified_bank_monitoring"
)

COLUMNS = [
    "CERT",
    "BANK_NAME",
    "report_date",
    "DEP",
    "ASSET",
    "LNLSGR",
    "ROA",
    "ROE",
    "NCLNLSR",
    "RBCRWAJ",
    "deposit_change_pct",
    "asset_change_pct",
    "roa_change_pp",
    "credit_change_pp",
    "capital_change_pp",
    "alert_count",
    "investigation_priority",
]


def build_history_query():
    """Read historical financials and Gold monitoring data."""

    return f"""
        SELECT
            f.CERT,
            f.BANK_NAME,
            f.report_date,
            f.DEP,
            f.ASSET,
            f.LNLSGR,
            f.ROA,
            f.ROE,
            f.NCLNLSR,
            f.RBCRWAJ,
            g.deposit_change_pct,
            g.asset_change_pct,
            g.roa_change_pp,
            g.credit_change_pp,
            g.capital_change_pp,
            g.alert_count,
            g.investigation_priority
        FROM {SILVER_TABLE} f
        LEFT JOIN {GOLD_VIEW} g
            ON f.CERT = g.CERT
           AND f.report_date = g.report_date
        ORDER BY f.report_date, f.CERT
    """


def validate_history(records):
    """
    Validate historical records before any S3 write.

    One bank can appear in multiple quarters,
    but not twice in the same quarter.
    """

    if not records:
        raise ValueError(
            "Historical snapshot is empty"
        )

    seen = set()
    quarter_counts = Counter()

    valid_priorities = {
        "HIGH_REVIEW_PRIORITY",
        "MEDIUM_REVIEW_PRIORITY",
        "LOW_REVIEW_PRIORITY",
        "INSUFFICIENT_DATA",
        "NO_ALERT",
    }

    for record in records:
        missing = set(COLUMNS) - set(record)

        if missing:
            raise ValueError(
                f"Missing history columns: {sorted(missing)}"
            )

        cert = record["CERT"]
        date = record["report_date"]

        if not cert:
            raise ValueError("Missing CERT")

        if not re.fullmatch(r"\d{8}", date):
            raise ValueError(
                f"Invalid report_date: {date}"
            )

        key = (cert, date)

        if key in seen:
            raise ValueError(
                f"Duplicate bank-quarter: {key}"
            )

        seen.add(key)
        quarter_counts[date] += 1

        if not record["BANK_NAME"]:
            raise ValueError(
                f"Missing bank name for {key}"
            )

        if record["investigation_priority"] not in (
            valid_priorities
        ):
            raise ValueError(
                f"Invalid investigation priority: {key}"
            )

        try:
            alert_count = int(record["alert_count"])
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid alert_count for {key}"
            ) from None

        if not 0 <= alert_count <= 5:
            raise ValueError(
                f"Alert count outside expected range: {key}"
            )

    dates = sorted(quarter_counts)

    print("\nHISTORICAL DATA VALIDATION")
    print(f"Total records: {len(records):,}")
    print(f"Quarters: {len(dates)}")
    print(f"First quarter: {dates[0]}")
    print(f"Latest quarter: {dates[-1]}")

    return {
        "total_records": len(records),
        "quarter_counts": dict(
            sorted(quarter_counts.items())
        ),
        "first_report_date": dates[0],
        "latest_report_date": dates[-1],
        "quarters": len(dates),
    }


def create_csv(records):
    """Serialize the historical dataset."""

    buffer = io.StringIO()

    writer = csv.DictWriter(
        buffer,
        fieldnames=COLUMNS,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(records)

    return buffer.getvalue().encode("utf-8")


def read_current_manifest(s3):
    """Read the existing historical snapshot reference."""

    try:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=LATEST_KEY,
        )
    except ClientError as error:
        code = error.response["Error"]["Code"]

        if code in ("NoSuchKey", "404"):
            return None

        raise

    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    current_date = manifest.get("latest_report_date")

    if not isinstance(current_date, str) or not re.fullmatch(
        r"\d{8}", current_date
    ):
        raise ValueError(
            "Invalid existing history manifest"
        )

    return manifest


def check_freshness(new_date, current_manifest):
    """Prevent publication of older historical coverage."""

    if current_manifest is None:
        return

    if new_date < current_manifest["latest_report_date"]:
        raise ValueError(
            "Historical snapshot rollback blocked"
        )


def publish_history():
    """Publish the validated historical dataset to S3."""

    athena = boto3.client(
        "athena",
        region_name=REGION,
    )

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    print("DATAOPS SENTINEL — HISTORICAL SNAPSHOT")
    print("=" * 45)

    # Read expected row counts independently from Silver.
    expected_rows = fetch_rows(
        athena,
        f"""
        SELECT
            report_date,
            COUNT(*) AS total_records,
            COUNT(DISTINCT CERT) AS unique_banks
        FROM {SILVER_TABLE}
        GROUP BY report_date
        ORDER BY report_date
        """,
    )

    expected_counts = {}

    for row in expected_rows:
        date = row["report_date"]
        count = int(row["total_records"])
        unique = int(row["unique_banks"])

        if count != unique:
            raise ValueError(
                f"Duplicate Silver records: {date}"
            )

        if date in expected_counts:
            raise ValueError(
                f"Duplicate Silver quarter: {date}"
            )

        expected_counts[date] = count

    if not expected_counts:
        raise ValueError("No Silver history found")

    current = read_current_manifest(s3)

    check_freshness(
        max(expected_counts),
        current,
    )

    print("Extracting historical Gold data...")

    records = fetch_rows(
        athena,
        build_history_query(),
    )

    summary = validate_history(records)

    # Compare EVERY quarter against the Silver source.
    if summary["quarter_counts"] != dict(
        sorted(expected_counts.items())
    ):
        raise ValueError(
            "Historical Gold/Silver quarter counts mismatch"
        )

    csv_bytes = create_csv(records)

    checksum = hashlib.sha256(
        csv_bytes
    ).hexdigest()

    snapshot_key = (
        f"{PREFIX}/snapshots/"
        f"latest_quarter={summary['latest_report_date']}/"
        f"{checksum}.csv"
    )

    manifest = {
        **summary,
        "published_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "snapshot_key": snapshot_key,
        "sha256": checksum,
        "source_view": GOLD_VIEW,
    }

    print("Uploading validated history to S3...")

    s3.put_object(
        Bucket=BUCKET,
        Key=snapshot_key,
        Body=csv_bytes,
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )

    uploaded = s3.head_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    if uploaded["ContentLength"] != len(csv_bytes):
        raise RuntimeError(
            "Historical snapshot upload size mismatch"
        )

    # Recheck before advancing the pointer.
    check_freshness(
        summary["latest_report_date"],
        read_current_manifest(s3),
    )

    # The pointer is updated only after validation
    # and snapshot upload succeed.
    s3.put_object(
        Bucket=BUCKET,
        Key=LATEST_KEY,
        Body=json.dumps(
            manifest,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    print("\nHISTORICAL SNAPSHOT PUBLISHED")
    print(f"Records: {summary['total_records']:,}")
    print(f"Quarters: {summary['quarters']}")
    print(f"Snapshot: {snapshot_key}")
    print(f"Manifest: {LATEST_KEY}")

    return manifest


def main():
    parser = argparse.ArgumentParser()

    modes = parser.add_mutually_exclusive_group(
        required=True
    )

    modes.add_argument(
        "--plan",
        action="store_true",
        help="Inspect configuration without AWS requests",
    )

    modes.add_argument(
        "--publish",
        action="store_true",
        help="Publish validated historical data",
    )

    args = parser.parse_args()

    if args.plan:
        print("HISTORICAL DASHBOARD PLAN")
        print(f"Source: {GOLD_VIEW}")
        print(f"Workgroup: {WORKGROUP}")
        print(f"Region: {REGION}")
        print(f"Bucket: {BUCKET}")
        print(f"Manifest: {LATEST_KEY}")
        print("No AWS requests made.")
        return

    publish_history()


if __name__ == "__main__":
    main()
