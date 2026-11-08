
import argparse
import json
import subprocess
import sys

import boto3

from src.pipeline.plan_quarterly_ingestion import (
    check_live_availability,
)
from src.pipeline.check_fdic_new_quarter import (
    validate_quarter,
)


BUCKET = "dataops-sentinel-lake-ikana-2026"
REGION = "ca-central-1"


def build_commands(quarter):
    """Build the existing one-quarter pipeline commands."""

    quarter = validate_quarter(quarter)

    return [
        [
            sys.executable,
            "-m",
            "src.ingestion.historical_backfill",
            "--start",
            quarter,
            "--end",
            quarter,
        ],
        [
            sys.executable,
            "-m",
            "src.pipeline.run_historical_silver",
            "--start",
            quarter,
            "--end",
            quarter,
            "--publish",
            "--bucket",
            BUCKET,
            "--region",
            REGION,
        ],
        [
            sys.executable,
            "-m",
            "src.pipeline.publish_named_silver",
            "--bucket",
            BUCKET,
            "--region",
            REGION,
            "--start",
            quarter,
            "--end",
            quarter,
        ],
    ]


def run_commands(commands):
    """
    Run pipeline stages sequentially.

    A nonzero exit stops processing immediately.
    """

    for index, command in enumerate(commands, start=1):
        print(
            f"\nRunning pipeline stage {index}",
            flush=True,
        )

        subprocess.run(
            command,
            check=True,
        )


def verify_named_publication(s3, quarter):
    """
    Verify a successful named Silver completion manifest.

    The manifest must correspond to the requested quarter.
    """

    prefix = (
        "silver/fdic/financials/"
        f"named-manifests/{quarter}-{quarter}/"
    )

    paginator = s3.get_paginator("list_objects_v2")

    keys = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]

            if key.endswith(".json"):
                keys.append(key)

    if not keys:
        raise RuntimeError(
            f"No named Silver completion manifest: {quarter}"
        )

    # Look for a successful manifest for this quarter.
    for key in sorted(keys, reverse=True):
        response = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )

        manifest = json.loads(
            response["Body"].read().decode("utf-8")
        )

        completed = manifest.get("completed", [])

        valid = (
            manifest.get("status") == "SUCCESS"
            and manifest.get("start_date") == quarter
            and manifest.get("end_date") == quarter
            and manifest.get("quarters_requested") == 1
            and manifest.get("quarters_completed") == 1
            and manifest.get("quarters_failed") == 0
            and len(completed) == 1
            and completed[0].get("report_date") == quarter
            and completed[0].get("names_missing") == 0
            and completed[0].get("output_records", 0) > 0
            and completed[0].get("output_records")
                == completed[0].get("source_records")
        )

        if valid:
            parquet_key = (
                "silver/fdic/financials/named/"
                f"report_date={quarter}/records.parquet"
            )

            metadata = s3.head_object(
                Bucket=BUCKET,
                Key=parquet_key,
            )

            if metadata["ContentLength"] <= 0:
                raise RuntimeError(
                    "Published named Parquet is empty"
                )

            print(
                f"Named Silver verified: {quarter}"
            )

            return manifest

    raise RuntimeError(
        f"No valid named Silver manifest for {quarter}"
    )


def run_pipeline(execute=False, approved=False):
    """Plan or execute one new reporting quarter."""

    plan = check_live_availability()

    print("\nDATAOPS SENTINEL — ONE-QUARTER PIPELINE")
    print("=" * 48)
    print(f"Status: {plan['status']}")

    if not plan["ingestion_required"]:
        print("No new quarter. Nothing to process.")
        return {"status": "UP_TO_DATE"}

    quarter = plan["next_quarter"]
    commands = build_commands(quarter)

    print(f"Target quarter: {quarter}")

    if not execute:
        print("\nPLANNED STAGES")

        for index, command in enumerate(commands, start=1):
            print(
                f"{index}. {' '.join(command)}"
            )

        print("\nNo ingestion or uploads performed.")

        return {
            "status": "PLAN_READY",
            "quarter": quarter,
        }

    if not approved:
        raise ValueError(
            "Execution requires explicit approval"
        )

    run_commands(commands)

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    manifest = verify_named_publication(
        s3,
        quarter,
    )

    print("\nONE-QUARTER SILVER PIPELINE PASSED")
    print(f"Quarter: {quarter}")
    print(
        "Named records: "
        f"{manifest['total_named_records']}"
    )

    print(
        "Athena registration and dashboard "
        "publication have NOT run."
    )

    return {
        "status": "SILVER_PUBLISHED",
        "quarter": quarter,
    }


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--plan",
        action="store_true",
        help="Check availability and display planned stages",
    )

    mode.add_argument(
        "--execute",
        action="store_true",
        help="Process one quarter and publish Silver",
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Approve live processing",
    )

    args = parser.parse_args()

    if args.execute and not args.yes:
        parser.error(
            "--execute requires --yes"
        )

    run_pipeline(
        execute=args.execute,
        approved=args.yes,
    )


if __name__ == "__main__":
    main()
