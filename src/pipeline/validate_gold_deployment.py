
import sys

import boto3

from src.pipeline.deploy_gold_views import (
    DATABASE,
    WORKGROUP,
    REGION,
    execute_athena_query,
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
        COALESCE(s.report_date, g.report_date) AS report_date,
        s.row_count AS silver_count,
        g.row_count AS gold_count,
        g.unique_banks
    FROM silver s
    FULL OUTER JOIN gold g
        ON s.report_date = g.report_date
)
SELECT
    COUNT(*) AS quarters,
    COALESCE(SUM(silver_count), 0) AS silver_records,
    COALESCE(SUM(gold_count), 0) AS gold_records,
    COUNT_IF(
        silver_count IS NULL
        OR gold_count IS NULL
        OR silver_count <> gold_count
        OR gold_count <> unique_banks
    ) AS failed_quarters
FROM comparison
"""


def validate():
    client = boto3.client(
        "athena",
        region_name=REGION,
    )

    query_id = execute_athena_query(
        client,
        VALIDATION_SQL,
        "gold_historical_validation",
    )

    result = client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=10,
    )

    rows = result["ResultSet"]["Rows"]

    if len(rows) < 2:
        raise RuntimeError(
            "Validation query returned no data"
        )

    values = [
        int(cell["VarCharValue"])
        for cell in rows[1]["Data"]
    ]

    quarters, silver, gold, failures = values

    print("\nGOLD DEPLOYMENT VALIDATION")
    print(f"Quarters: {quarters}")
    print(f"Silver records: {silver}")
    print(f"Gold records: {gold}")
    print(f"Failed quarters: {failures}")

    if (
        quarters != 22
        or silver != 102983
        or gold != 102983
        or failures != 0
    ):
        raise RuntimeError(
            "Gold deployment validation FAILED"
        )

    print("GOLD DEPLOYMENT VALIDATION PASSED")


if __name__ == "__main__":
    try:
        validate()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
