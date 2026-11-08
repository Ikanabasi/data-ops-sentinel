
import argparse
import json
import re

import boto3
from botocore.exceptions import ClientError

from src.pipeline.deploy_gold_views import (
    DATABASE,
    REGION,
    execute_athena_query,
)
from src.pipeline.check_fdic_new_quarter import (
    validate_quarter,
)


BUCKET = "dataops-sentinel-lake-ikana-2026"

TABLE = "fdic_financials_named"

GOLD_VIEWS = [
    "gold_bank_deposit_alerts",
    "gold_bank_asset_trends",
    "gold_bank_profitability",
    "gold_bank_credit_quality",
    "gold_bank_capital_adequacy",
    "gold_unified_bank_monitoring",
]


def get_parquet_key(quarter):
    quarter = validate_quarter(quarter)

    return (
        "silver/fdic/financials/named/"
        f"report_date={quarter}/records.parquet"
    )


def verify_silver_object(s3, quarter):
    """Check that the named Silver Parquet object exists."""

    key = get_parquet_key(quarter)

    metadata = s3.head_object(
        Bucket=BUCKET,
        Key=key,
    )

    size = metadata["ContentLength"]

    if size <= 0:
        raise RuntimeError(
            "Named Silver Parquet file is empty"
        )

    print(f"Silver Parquet exists: {key}")
    print(f"Object size: {size:,} bytes")

    return key


def verify_athena_partition(glue, quarter):
    """Verify the exact named Silver partition."""

    quarter = validate_quarter(quarter)

    try:
        partition = glue.get_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionValues=[quarter],
        )

    except ClientError as error:
        code = error.response["Error"]["Code"]

        if code == "EntityNotFoundException":
            raise RuntimeError(
                f"Athena partition {quarter} is not "
                "registered. Registration is required "
                "before Gold can be validated."
            ) from error

        raise

    location = partition["Partition"][
        "StorageDescriptor"
    ]["Location"].rstrip("/")

    expected_location = (
        f"s3://{BUCKET}/"
        "silver/fdic/financials/named/"
        f"report_date={quarter}"
    )

    if location != expected_location:
        raise RuntimeError(
            "Partition points to unexpected location: "
            f"{location}"
        )

    print(f"Athena partition verified: {quarter}")

    return location


def build_validation_query(quarter):
    """
    Compare Silver and each monitoring view.

    Also check unified Gold classifications.
    """

    quarter = validate_quarter(quarter)

    sources = [
        TABLE,
        *GOLD_VIEWS,
    ]

    parts = []

    for index, source in enumerate(sources):
        parts.append(
            f"""
            SELECT
                '{source}' AS source_name,
                COUNT(*) AS total_records,
                COUNT(DISTINCT CERT) AS unique_banks,
                COUNT_IF(CERT IS NULL) AS missing_certs
            FROM {DATABASE}.{source}
            WHERE report_date = '{quarter}'
            """
        )

    return "\nUNION ALL\n".join(parts)


def build_quality_query(quarter):
    """Check the unified monitoring output."""

    quarter = validate_quarter(quarter)

    return f"""
        SELECT
            COUNT(*) AS total_records,

            COUNT_IF(
                BANK_NAME IS NULL
                OR TRIM(BANK_NAME) = ''
            ) AS missing_names,

            COUNT_IF(
                alert_count IS NULL
                OR alert_count < 0
                OR alert_count > 5
            ) AS invalid_alert_counts,

            COUNT_IF(
                investigation_priority IS NULL
                OR investigation_priority NOT IN (
                    'HIGH_REVIEW_PRIORITY',
                    'MEDIUM_REVIEW_PRIORITY',
                    'LOW_REVIEW_PRIORITY',
                    'INSUFFICIENT_DATA',
                    'NO_ALERT'
                )
                OR (
                    alert_count >= 3
                    AND investigation_priority <>
                        'HIGH_REVIEW_PRIORITY'
                )
                OR (
                    alert_count = 2
                    AND investigation_priority <>
                        'MEDIUM_REVIEW_PRIORITY'
                )
                OR (
                    alert_count = 1
                    AND investigation_priority <>
                        'LOW_REVIEW_PRIORITY'
                )
                OR (
                    alert_count = 0
                    AND has_data_limitation = TRUE
                    AND investigation_priority <>
                        'INSUFFICIENT_DATA'
                )
                OR (
                    alert_count = 0
                    AND has_data_limitation = FALSE
                    AND investigation_priority <> 'NO_ALERT'
                )
            ) AS invalid_priorities,

            COUNT_IF(
                deposit_alert IS NULL
                OR asset_alert IS NULL
                OR profitability_alert IS NULL
                OR credit_alert IS NULL
                OR capital_alert IS NULL
                OR has_data_limitation IS NULL
            ) AS missing_signal_values

        FROM {DATABASE}.gold_unified_bank_monitoring
        WHERE report_date = '{quarter}'
    """


def fetch_query_rows(athena, sql, description):
    """Execute one validation query and return its rows."""

    query_id = execute_athena_query(
        athena,
        sql,
        description,
    )

    response = athena.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=20,
    )

    rows = response["ResultSet"]["Rows"]

    if len(rows) < 2:
        raise RuntimeError(
            f"No validation results for {description}"
        )

    return [
        [
            cell.get("VarCharValue", "")
            for cell in row["Data"]
        ]
        for row in rows[1:]
    ]



def evaluate_source_counts(rows):
    """
    Validate Silver and Gold coverage.

    Full-coverage views must match Silver exactly.

    Eligible-comparison views may have fewer records,
    but must not contain duplicates, missing CERTs,
    or more records than Silver.
    """

    full_coverage_sources = {
        TABLE,
        "gold_bank_asset_trends",
        "gold_bank_profitability",
        "gold_bank_credit_quality",
        "gold_bank_capital_adequacy",
        "gold_unified_bank_monitoring",
    }

    comparison_sources = {
        "gold_bank_deposit_alerts",
    }

    expected_sources = (
        full_coverage_sources | comparison_sources
    )

    observed = {}

    for row in rows:
        if len(row) != 4:
            raise RuntimeError(
                "Unexpected source validation result"
            )

        source, total, unique, missing = row

        if source in observed:
            raise RuntimeError(
                f"Duplicate validation source: {source}"
            )

        observed[source] = {
            "total": int(total),
            "unique": int(unique),
            "missing": int(missing),
        }

    if set(observed) != expected_sources:
        raise RuntimeError(
            "Missing or unexpected validation sources"
        )

    silver_count = observed[TABLE]["total"]

    if silver_count <= 0:
        raise RuntimeError(
            "Silver has no records for this quarter"
        )

    for source, counts in observed.items():

        if counts["missing"] != 0:
            raise RuntimeError(
                f"Missing CERT values in {source}"
            )

        if counts["unique"] != counts["total"]:
            raise RuntimeError(
                f"Duplicate CERT values in {source}"
            )

        if source in full_coverage_sources:
            if counts["total"] != silver_count:
                raise RuntimeError(
                    f"Row count mismatch for {source}: "
                    f"{counts['total']} vs {silver_count}"
                )

        elif source in comparison_sources:
            if counts["total"] > silver_count:
                raise RuntimeError(
                    f"Comparison view has too many "
                    f"records: {source}"
                )

    print("\nSOURCE COVERAGE VALIDATION PASSED")

    for source, counts in observed.items():
        print(
            f"{source}: {counts['total']:,} records"
        )

    return silver_count


def evaluate_quality(rows, expected_count):
    """Require valid unified monitoring output."""

    if len(rows) != 1 or len(rows[0]) != 5:
        raise RuntimeError(
            "Unexpected quality validation result"
        )

    values = [int(value) for value in rows[0]]

    total, missing_names, invalid_alerts, (
        invalid_priorities
    ), missing_signals = values

    if total != expected_count:
        raise RuntimeError(
            "Unified Gold record count mismatch"
        )

    if any([
        missing_names,
        invalid_alerts,
        invalid_priorities,
        missing_signals,
    ]):
        raise RuntimeError(
            "Gold quality validation failed: "
            f"missing_names={missing_names}, "
            f"invalid_alerts={invalid_alerts}, "
            f"invalid_priorities={invalid_priorities}, "
            f"missing_signals={missing_signals}"
        )


def check_readiness(quarter):
    """Run all Gold readiness checks."""

    quarter = validate_quarter(quarter)

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    glue = boto3.client(
        "glue",
        region_name=REGION,
    )

    athena = boto3.client(
        "athena",
        region_name=REGION,
    )

    print("\nDATAOPS SENTINEL — GOLD READINESS")
    print("=" * 45)
    print(f"Quarter: {quarter}")

    verify_silver_object(s3, quarter)

    verify_athena_partition(glue, quarter)

    print("Validating Silver and Gold row counts...")

    source_rows = fetch_query_rows(
        athena,
        build_validation_query(quarter),
        "gold_source_reconciliation",
    )

    record_count = evaluate_source_counts(
        source_rows
    )

    print("Validating monitoring classifications...")

    quality_rows = fetch_query_rows(
        athena,
        build_quality_query(quarter),
        "gold_monitoring_quality",
    )

    evaluate_quality(
        quality_rows,
        record_count,
    )

    print("\nGOLD READINESS PASSED")
    print(f"Quarter: {quarter}")
    print(f"Validated records: {record_count:,}")

    return {
        "status": "READY",
        "quarter": quarter,
        "records": record_count,
    }


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--plan",
        action="store_true",
        help="Check configuration without AWS access",
    )

    mode.add_argument(
        "--check",
        metavar="YYYYMMDD",
        help="Verify a reporting quarter in AWS",
    )

    args = parser.parse_args()

    if args.plan:
        print("DATAOPS SENTINEL GOLD READINESS PLAN")
        print(f"Database: {DATABASE}")
        print(f"Bucket: {BUCKET}")
        print(f"Sources: {len(GOLD_VIEWS) + 1}")
        print("No AWS requests made.")
        return

    result = check_readiness(args.check)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
