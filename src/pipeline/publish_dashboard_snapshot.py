
import argparse
import csv
import hashlib
import io
import json
import re
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.pipeline.deploy_gold_views import (
    REGION,
    WORKGROUP,
    execute_athena_query,
)


# ==========================================================
# CONFIGURATION
# ==========================================================

BUCKET = "dataops-sentinel-lake-ikana-2026"
PREFIX = "dashboard/gold"
LATEST_KEY = f"{PREFIX}/latest.json"

SOURCE_VIEW = (
    "dataops_sentinel.gold_unified_bank_monitoring"
)

SILVER_TABLE = (
    "dataops_sentinel.fdic_financials_named"
)

COLUMNS = [
    "CERT",
    "BANK_NAME",
    "report_date",
    "deposit_change_pct",
    "asset_change_pct",
    "current_roa",
    "current_roe",
    "roa_change_pp",
    "credit_change_pp",
    "current_capital_ratio",
    "capital_change_pp",
    "deposit_alert",
    "asset_alert",
    "profitability_alert",
    "credit_alert",
    "capital_alert",
    "alert_count",
    "has_data_limitation",
    "investigation_priority",
]

PRIORITIES = {
    0: {
        "NO_ALERT",
        "INSUFFICIENT_DATA",
    },
    1: {"LOW_REVIEW_PRIORITY"},
    2: {"MEDIUM_REVIEW_PRIORITY"},
    3: {"HIGH_REVIEW_PRIORITY"},
    4: {"HIGH_REVIEW_PRIORITY"},
    5: {"HIGH_REVIEW_PRIORITY"},
}


# ==========================================================
# ATHENA DATA EXTRACTION
# ==========================================================

def fetch_rows(client, sql):
    """
    Execute an Athena query and retrieve every result page.

    Athena returns query results in pages, so pagination
    ensures we collect all banks rather than the first 1,000.
    """

    query_id = execute_athena_query(
        client,
        sql,
        "dashboard_snapshot",
    )

    paginator = client.get_paginator(
        "get_query_results"
    )

    headers = None
    records = []
    first_row = True

    for page in paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={"PageSize": 1000},
    ):
        column_info = page["ResultSet"][
            "ResultSetMetadata"
        ]["ColumnInfo"]

        names = [
            column["Name"]
            for column in column_info
        ]

        if headers is None:
            headers = names

        for row in page["ResultSet"]["Rows"]:
            values = [
                cell.get("VarCharValue", "")
                for cell in row["Data"]
            ]

            # Athena SELECT output includes a header row.
            # Skip it once, even when the query returns
            # no actual data.
            if first_row:
                first_row = False

                if values == names:
                    continue

            records.append(
                dict(zip(names, values))
            )

    return records


# ==========================================================
# SNAPSHOT DATA VALIDATION
# ==========================================================

def validate_records(
    records,
    report_date,
    expected_count,
):
    """
    Validate the Gold records before publishing.

    A failed validation prevents any new snapshot data
    from being uploaded.
    """

    if not records:
        raise ValueError(
            "Snapshot contains no records"
        )

    if len(records) != expected_count:
        raise ValueError(
            f"Row mismatch: expected {expected_count}, "
            f"got {len(records)}"
        )

    seen = set()

    for record in records:
        missing = set(COLUMNS) - set(record)

        if missing:
            raise ValueError(
                f"Missing snapshot columns: {missing}"
            )

        cert = record["CERT"]

        if not cert or cert in seen:
            raise ValueError(
                f"Missing or duplicate CERT: {cert}"
            )

        seen.add(cert)

        if record["report_date"] != report_date:
            raise ValueError(
                "Mixed reporting quarters"
            )

        if not record["BANK_NAME"]:
            raise ValueError(
                f"Missing bank name for {cert}"
            )

        try:
            alerts = int(record["alert_count"])
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid alert count for bank {cert}"
            ) from None

        if alerts not in PRIORITIES:
            raise ValueError(
                f"Invalid alert count: {alerts}"
            )

        priority = record[
            "investigation_priority"
        ]

        if priority not in PRIORITIES[alerts]:
            raise ValueError(
                f"Invalid priority for bank {cert}"
            )

        limitation = str(
            record["has_data_limitation"]
        ).lower()

        if limitation not in ("true", "false"):
            raise ValueError(
                f"Invalid data limitation: {cert}"
            )

        if alerts == 0:
            expected_priority = (
                "INSUFFICIENT_DATA"
                if limitation == "true"
                else "NO_ALERT"
            )

            if priority != expected_priority:
                raise ValueError(
                    f"Inconsistent priority for {cert}"
                )

    print(
        f"VALIDATION PASSED: "
        f"{len(records):,} unique bank records"
    )


# ==========================================================
# CSV CREATION
# ==========================================================

def create_csv(records):
    """
    Convert validated records into CSV bytes.

    The CSV will be stored as a versioned S3 object.
    """

    buffer = io.StringIO()

    writer = csv.DictWriter(
        buffer,
        fieldnames=COLUMNS,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(records)

    return buffer.getvalue().encode("utf-8")


# ==========================================================
# CURRENT SNAPSHOT LOOKUP
# ==========================================================

def get_current_snapshot(s3):
    """
    Retrieve the currently published latest.json manifest.

    Return None only when the snapshot does not exist.

    Other AWS errors must not be silently ignored.
    """

    try:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=LATEST_KEY,
        )

    except ClientError as error:
        error_code = error.response[
            "Error"
        ]["Code"]

        if error_code in (
            "NoSuchKey",
            "404",
        ):
            return None

        raise

    manifest = json.loads(
        response["Body"]
        .read()
        .decode("utf-8")
    )

    current_date = manifest.get(
        "report_date"
    )

    if (
        not isinstance(current_date, str)
        or not re.fullmatch(
            r"\d{8}",
            current_date,
        )
    ):
        raise ValueError(
            "Existing latest.json has an "
            "invalid report_date"
        )

    return manifest


# ==========================================================
# PREVENT OLDER SNAPSHOT PUBLICATION
# ==========================================================

def validate_snapshot_freshness(
    new_report_date,
    current_manifest,
):
    """
    Prevent an older quarter from replacing newer data.

    Publishing the same quarter remains allowed because
    revised banking data may become available.
    """

    if not re.fullmatch(
        r"\d{8}",
        new_report_date,
    ):
        raise ValueError(
            "Invalid new report_date"
        )

    if current_manifest is None:
        return

    current_date = current_manifest[
        "report_date"
    ]

    if new_report_date < current_date:
        raise ValueError(
            f"Snapshot rollback blocked: "
            f"new quarter {new_report_date} "
            f"is older than published "
            f"quarter {current_date}"
        )


# ==========================================================
# S3 SNAPSHOT PUBLICATION
# ==========================================================

def publish_snapshot():
    """
    Publish the latest validated Gold snapshot.

    Sequence:
        1. Identify latest Silver quarter.
        2. Compare against currently published quarter.
        3. Retrieve Gold records.
        4. Validate the snapshot.
        5. Generate CSV and checksum.
        6. Upload versioned CSV to S3.
        7. Verify the uploaded file.
        8. Update latest.json last.

    This protects the existing dashboard snapshot
    when an earlier step fails.
    """

    athena = boto3.client(
        "athena",
        region_name=REGION,
    )

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    print("\nDATAOPS SENTINEL DASHBOARD PUBLISHER")
    print("=" * 45)

    print("Checking latest Silver quarter...")

    latest = fetch_rows(
        athena,
        f"""
        SELECT
            report_date,
            COUNT(*) AS total_records,
            COUNT(DISTINCT CERT) AS unique_banks
        FROM {SILVER_TABLE}
        GROUP BY report_date
        ORDER BY report_date DESC
        LIMIT 1
        """,
    )

    if len(latest) != 1:
        raise ValueError(
            "Cannot identify latest Silver quarter"
        )

    report_date = latest[0][
        "report_date"
    ]

    if not re.fullmatch(
        r"\d{8}",
        report_date,
    ):
        raise ValueError(
            "Invalid report_date"
        )

    expected_count = int(
        latest[0]["total_records"]
    )

    unique_banks = int(
        latest[0]["unique_banks"]
    )

    if expected_count != unique_banks:
        raise ValueError(
            "Duplicate bank IDs in Silver"
        )

    print(f"Latest quarter: {report_date}")
    print(f"Expected banks: {expected_count:,}")

    # ------------------------------------------------------
    # CHECK CURRENT PUBLISHED SNAPSHOT
    # ------------------------------------------------------

    current_manifest = get_current_snapshot(
        s3
    )

    validate_snapshot_freshness(
        report_date,
        current_manifest,
    )

    if current_manifest is None:
        print(
            "No previous dashboard snapshot found."
        )
    else:
        print(
            "Previously published quarter: "
            f"{current_manifest['report_date']}"
        )

    # ------------------------------------------------------
    # EXTRACT GOLD RECORDS
    # ------------------------------------------------------

    print("Extracting Gold monitoring data...")

    sql = f"""
        SELECT {", ".join(COLUMNS)}
        FROM {SOURCE_VIEW}
        WHERE report_date = '{report_date}'
        ORDER BY CERT
    """

    records = fetch_rows(
        athena,
        sql,
    )

    # ------------------------------------------------------
    # VALIDATE BEFORE ANY S3 WRITES
    # ------------------------------------------------------

    validate_records(
        records,
        report_date,
        expected_count,
    )

    # ------------------------------------------------------
    # GENERATE CSV AND CHECKSUM
    # ------------------------------------------------------

    csv_bytes = create_csv(
        records
    )

    checksum = hashlib.sha256(
        csv_bytes
    ).hexdigest()

    published_at = datetime.now(
        timezone.utc
    ).isoformat()

    snapshot_key = (
        f"{PREFIX}/snapshots/"
        f"report_date={report_date}/"
        f"{checksum}.csv"
    )

    manifest = {
        "report_date": report_date,
        "published_at": published_at,
        "total_records": len(records),
        "unique_banks": len(records),
        "snapshot_key": snapshot_key,
        "sha256": checksum,
        "source_view": SOURCE_VIEW,
    }

    # ------------------------------------------------------
    # UPLOAD VALIDATED VERSIONED SNAPSHOT
    # ------------------------------------------------------

    print("Uploading validated snapshot to S3...")

    s3.put_object(
        Bucket=BUCKET,
        Key=snapshot_key,
        Body=csv_bytes,
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )

    # ------------------------------------------------------
    # VERIFY UPLOADED OBJECT SIZE
    # ------------------------------------------------------

    uploaded = s3.head_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    if uploaded["ContentLength"] != len(
        csv_bytes
    ):
        raise RuntimeError(
            "Uploaded snapshot size mismatch"
        )

    print("Snapshot upload verified.")

    # ------------------------------------------------------
    # PUBLISH latest.json LAST
    # ------------------------------------------------------

    # Recheck freshness just before publishing the pointer.
    # This reduces rollback risk but is not a fully atomic
    # concurrency guard.
    latest_manifest = get_current_snapshot(s3)

    validate_snapshot_freshness(
        report_date,
        latest_manifest,
    )

    print("Publishing latest.json...")

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

    print("\nDASHBOARD SNAPSHOT PUBLISHED")
    print("=" * 45)
    print(f"Quarter: {report_date}")
    print(f"Records: {len(records):,}")
    print(f"Snapshot: {snapshot_key}")
    print(f"Latest pointer: {LATEST_KEY}")
    print(f"SHA256: {checksum}")
    print("Status: SUCCESS")

    return manifest


# ==========================================================
# COMMAND LINE INTERFACE
# ==========================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "DataOps Sentinel automated "
            "dashboard snapshot publisher"
        )
    )

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--plan",
        action="store_true",
        help=(
            "Display configuration "
            "without AWS access"
        ),
    )

    mode.add_argument(
        "--publish",
        action="store_true",
        help=(
            "Publish a validated dashboard "
            "snapshot to S3"
        ),
    )

    args = parser.parse_args()

    if args.plan:
        print("\nDASHBOARD REFRESH PLAN")
        print("=" * 45)
        print(f"Workgroup: {WORKGROUP}")
        print(f"Region: {REGION}")
        print(f"Bucket: {BUCKET}")
        print(f"Pointer: {LATEST_KEY}")
        print(f"Source: {SOURCE_VIEW}")
        print("No AWS requests made.")
        return

    publish_snapshot()


if __name__ == "__main__":
    main()
