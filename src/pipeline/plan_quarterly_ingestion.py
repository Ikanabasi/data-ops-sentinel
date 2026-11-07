
import argparse
import calendar
import json
import os
from datetime import datetime
from pathlib import Path

import boto3

from src.pipeline.check_fdic_new_quarter import (
    fetch_latest_fdic_quarter,
    validate_quarter,
)

from src.pipeline.check_dashboard_refresh import (
    REGION,
    find_latest_silver_quarter,
)


def next_quarter(report_date):
    """
    Calculate the reporting quarter immediately
    after the supplied quarter.
    """

    validate_quarter(report_date)

    current = datetime.strptime(
        report_date, "%Y%m%d"
    )

    year = current.year
    month = current.month + 3

    if month > 12:
        month = 3
        year += 1

    day = calendar.monthrange(year, month)[1]

    next_date = f"{year}{month:02d}{day:02d}"

    return validate_quarter(next_date)


def evaluate_ingestion_plan(
    fdic_date,
    silver_date,
):
    """
    Decide whether exactly one new quarter
    can be processed.

    Never skip an unprocessed quarter.
    """

    fdic_date = validate_quarter(fdic_date)
    silver_date = validate_quarter(silver_date)

    if fdic_date < silver_date:
        raise ValueError(
            "FDIC reporting date is older than Silver"
        )

    if fdic_date == silver_date:
        return {
            "fdic_latest": fdic_date,
            "silver_latest": silver_date,
            "next_quarter": None,
            "ingestion_required": False,
            "status": "UP_TO_DATE",
        }

    target = next_quarter(silver_date)

    if target > fdic_date:
        raise ValueError(
            "Invalid reporting quarter sequence"
        )

    return {
        "fdic_latest": fdic_date,
        "silver_latest": silver_date,
        "next_quarter": target,
        "ingestion_required": True,
        "status": "READY_FOR_REVIEW",
    }


def print_plan(plan):
    """Display the ingestion decision."""

    print("\nDATAOPS SENTINEL — QUARTERLY INGESTION")
    print("=" * 48)

    for key, value in plan.items():
        print(f"{key}: {value}")

    if plan["ingestion_required"]:
        print(
            "\nA new quarter is eligible for processing."
        )
        print(
            "No ingestion or AWS writes have started."
        )
    else:
        print(
            "\nNo new reporting quarter to ingest."
        )


def write_github_outputs(plan):
    """Make validated values available to GitHub Actions."""

    output_path = os.getenv("GITHUB_OUTPUT")

    if not output_path:
        return

    with Path(output_path).open(
        "a",
        encoding="utf-8",
    ) as output:
        output.write(
            "ingestion_required="
            f"{str(plan['ingestion_required']).lower()}\n"
        )

        output.write(
            f"target_quarter="
            f"{plan['next_quarter'] or ''}\n"
        )

        output.write(
            f"plan_status={plan['status']}\n"
        )


def check_live_availability():
    """
    Check FDIC and named Silver availability.

    This operation is read-only.
    """

    fdic_date = fetch_latest_fdic_quarter()

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    silver_date = find_latest_silver_quarter(s3)

    plan = evaluate_ingestion_plan(
        fdic_date,
        silver_date,
    )

    print_plan(plan)
    write_github_outputs(plan)

    return plan


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Plan the next FDIC reporting-quarter "
            "ingestion safely"
        )
    )

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--plan",
        action="store_true",
        help="Show a simulated plan without network access",
    )

    mode.add_argument(
        "--check",
        action="store_true",
        help="Check live FDIC and S3 reporting dates",
    )

    args = parser.parse_args()

    if args.plan:
        print("SIMULATED NEXT-QUARTER EXAMPLE")

        example = evaluate_ingestion_plan(
            "20260930",
            "20260630",
        )

        print_plan(example)
        print("No network or AWS requests made.")
        return

    check_live_availability()


if __name__ == "__main__":
    main()

