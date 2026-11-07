
import argparse
import sys

import boto3

from src.pipeline.deploy_gold_views import (
    REGION,
    execute_athena_query,
)

from src.pipeline.check_fdic_new_quarter import (
    validate_quarter,
)


VALIDATION_SQL = """
WITH silver AS (
    SELECT
        report_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT CERT) AS unique_banks
    FROM dataops_sentinel.fdic_financials_named
    GROUP BY report_date
),
gold AS (
    SELECT
        report_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT CERT) AS unique_banks
    FROM dataops_sentinel.gold_unified_bank_monitoring
    GROUP BY report_date
),
comparison AS (
    SELECT
        COALESCE(s.report_date, g.report_date)
            AS report_date,

        s.row_count AS silver_count,
        s.unique_banks AS silver_unique,

        g.row_count AS gold_count,
        g.unique_banks AS gold_unique

    FROM silver s
    FULL OUTER JOIN gold g
        ON s.report_date = g.report_date
)
SELECT
    COUNT(*) AS quarters,

    COALESCE(SUM(silver_count), 0)
        AS silver_records,

    COALESCE(SUM(gold_count), 0)
        AS gold_records,

    COUNT_IF(
        silver_count IS NULL
        OR gold_count IS NULL
        OR silver_count <> gold_count
        OR silver_count <> silver_unique
        OR gold_count <> gold_unique
    ) AS failed_quarters,

    MAX(report_date) AS latest_quarter,

    MIN(report_date) AS first_quarter

FROM comparison
"""


def evaluate_validation(
    quarters,
    silver_records,
    gold_records,
    failed_quarters,
    latest_quarter,
    first_quarter,
    expected_quarter=None,
):
    """
    Evaluate Gold reconciliation without assuming
    a permanently fixed number of reporting quarters.
    """

    if quarters < 22:
        raise RuntimeError(
            f"Expected at least 22 historical quarters, "
            f"found {quarters}"
        )

    if first_quarter != "20210331":
        raise RuntimeError(
            "Unexpected first reporting quarter"
        )

    if silver_records <= 0:
        raise RuntimeError(
            "Silver contains no records"
        )

    if silver_records != gold_records:
        raise RuntimeError(
            "Silver and Gold record totals differ"
        )

    if failed_quarters != 0:
        raise RuntimeError(
            f"{failed_quarters} reporting quarters "
            "failed reconciliation"
        )

    if expected_quarter is not None:
        expected_quarter = validate_quarter(
            expected_quarter
        )

        if latest_quarter != expected_quarter:
            raise RuntimeError(
                f"Expected latest quarter "
                f"{expected_quarter}, "
                f"found {latest_quarter}"
            )

    return {
        "quarters": quarters,
        "silver_records": silver_records,
        "gold_records": gold_records,
        "latest_quarter": latest_quarter,
        "status": "PASSED",
    }


def validate(expected_quarter=None):
    """Run historical Gold reconciliation in Athena."""

    athena = boto3.client(
        "athena",
        region_name=REGION,
    )

    query_id = execute_athena_query(
        athena,
        VALIDATION_SQL,
        "gold_historical_validation",
    )

    response = athena.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=10,
    )

    rows = response["ResultSet"]["Rows"]

    if len(rows) < 2:
        raise RuntimeError(
            "Validation query returned no results"
        )

    values = [
        cell.get("VarCharValue", "")
        for cell in rows[1]["Data"]
    ]

    if len(values) != 6:
        raise RuntimeError(
            "Unexpected validation result format"
        )

    result = evaluate_validation(
        quarters=int(values[0]),
        silver_records=int(values[1]),
        gold_records=int(values[2]),
        failed_quarters=int(values[3]),
        latest_quarter=values[4],
        first_quarter=values[5],
        expected_quarter=expected_quarter,
    )

    print("\nDATAOPS SENTINEL — GOLD VALIDATION")
    print("=" * 45)

    for key, value in result.items():
        print(f"{key}: {value}")

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--expected-quarter",
        default=None,
        help=(
            "Optional required latest reporting quarter "
            "in YYYYMMDD format"
        ),
    )

    args = parser.parse_args()

    try:
        validate(
            expected_quarter=args.expected_quarter
        )
    except Exception as error:
        print(
            f"GOLD VALIDATION FAILED: {error}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
