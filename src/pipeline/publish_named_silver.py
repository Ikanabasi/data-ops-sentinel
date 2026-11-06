
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3

from src.ingestion.ingest_fdic_institutions import ingest_institutions
from src.transformation.transform_fdic_institutions import (
    transform_institutions,
)
from src.transformation.enrich_fdic_financials import enrich_quarter
from src.transformation.add_fdic_financial_names import enrich_bank_names
from src.pipeline.run_historical_name_enrichment import generate_quarters


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

FINANCIALS_ROOT = Path(
    "data/silver/fdic/financials/parquet"
)

NAMED_ROOT = Path(
    "data/silver/fdic/financials/named"
)

BUCKET_PREFIX = "silver/fdic/financials"


# --------------------------------------------------
# AWS S3 HELPERS
# --------------------------------------------------

def download_financial_parquet(s3, bucket, quarter):
    """Download one quarter of Silver data from S3."""

    source_key = (
        f"{BUCKET_PREFIX}/parquet/"
        f"report_date={quarter}/records.parquet"
    )

    local_file = (
        FINANCIALS_ROOT
        / f"report_date={quarter}"
        / "records.parquet"
    )

    local_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(f"Downloading: {source_key}")

    s3.download_file(
        bucket,
        source_key,
        str(local_file),
    )

    if not local_file.exists():
        raise FileNotFoundError(
            f"Downloaded file not found: {local_file}"
        )

    if local_file.stat().st_size == 0:
        raise ValueError(
            f"Downloaded file is empty: {local_file}"
        )

    return local_file


def upload_and_verify(s3, bucket, local_file, s3_key):
    """Upload a file and verify its size in S3."""

    if not local_file.exists():
        raise FileNotFoundError(local_file)

    local_size = local_file.stat().st_size

    if local_size == 0:
        raise ValueError(
            f"Cannot upload an empty file: {local_file}"
        )

    s3.upload_file(
        str(local_file),
        bucket,
        s3_key,
    )

    remote = s3.head_object(
        Bucket=bucket,
        Key=s3_key,
    )

    remote_size = remote["ContentLength"]

    if remote_size != local_size:
        raise ValueError(
            f"S3 size mismatch for {s3_key}: "
            f"local={local_size}, remote={remote_size}"
        )

    print(f"Verified S3 upload: {s3_key}")


# --------------------------------------------------
# MAIN CLOUD PIPELINE
# --------------------------------------------------

def publish_named_silver(bucket, region, start, end):
    """
    Download existing Silver financials from S3,
    enrich them with bank names and institution
    information, and publish named Silver data.
    """

    if not bucket:
        raise ValueError("S3 bucket is required")

    quarters = generate_quarters(start, end)

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    s3 = boto3.client(
        "s3",
        region_name=region,
    )

    print("\nFDIC NAMED SILVER CLOUD PIPELINE")
    print("=" * 50)
    print(f"Run ID: {run_id}")
    print(f"Bucket: {bucket}")
    print(f"Region: {region}")
    print(f"Quarters requested: {len(quarters)}")

    # Refresh the institution directory once,
    # not separately for every quarter.
    print("\nRefreshing FDIC institution directory")

    ingest_institutions()
    institution_quality = transform_institutions()

    if institution_quality["accepted_records"] == 0:
        raise ValueError(
            "Institution directory contains no valid records"
        )

    completed = []
    failed = []

    for index, quarter in enumerate(quarters, start=1):

        print(
            f"\n[{index}/{len(quarters)}] "
            f"Processing {quarter}"
        )

        try:
            # 1. Download the original Silver Parquet.
            download_financial_parquet(
                s3,
                bucket,
                quarter,
            )

            # 2. Add institution location and status.
            institution_report = enrich_quarter(
                quarter
            )

            # 3. Add reporting-quarter bank names.
            name_report = enrich_bank_names(
                quarter
            )

            source_count = institution_report[
                "source_records"
            ]

            output_count = name_report[
                "output_records"
            ]

            names_matched = name_report[
                "names_matched"
            ]

            names_missing = name_report[
                "names_missing"
            ]

            # 4. Enforce data-quality requirements.
            if source_count == 0:
                raise ValueError(
                    "Source financial dataset is empty"
                )

            if output_count != source_count:
                raise ValueError(
                    "Financial record count changed "
                    f"for {quarter}"
                )

            if names_matched != source_count:
                raise ValueError(
                    "Bank-name coverage is incomplete "
                    f"for {quarter}"
                )

            if names_missing != 0:
                raise ValueError(
                    f"{names_missing} missing bank names "
                    f"for {quarter}"
                )

            # 5. Locate the validated output files.
            named_dir = (
                NAMED_ROOT
                / f"report_date={quarter}"
            )

            parquet_file = (
                named_dir / "records.parquet"
            )

            report_file = (
                named_dir / "quality_report.json"
            )

            # 6. Upload named Parquet to S3.
            parquet_key = (
                f"{BUCKET_PREFIX}/named/"
                f"report_date={quarter}/records.parquet"
            )

            upload_and_verify(
                s3,
                bucket,
                parquet_file,
                parquet_key,
            )

            # 7. Upload the name-quality report.
            quality_key = (
                f"{BUCKET_PREFIX}/named-quality-reports/"
                f"{quarter}/quality_report.json"
            )

            upload_and_verify(
                s3,
                bucket,
                report_file,
                quality_key,
            )

            completed.append({
                "report_date": quarter,
                "source_records": source_count,
                "output_records": output_count,
                "names_matched": names_matched,
                "names_missing": names_missing,
                "institution_match_rate":
                    institution_report["match_rate"],
            })

            print(
                f"SUCCESS: {quarter} | "
                f"{output_count} named records"
            )

        except Exception as error:
            failed.append({
                "report_date": quarter,
                "error": str(error),
            })

            print(
                f"FAILED: {quarter} | {error}"
            )

            # Stop after the first failed quarter.
            break

    # --------------------------------------------------
    # FINAL BATCH MANIFEST
    # --------------------------------------------------

    status = (
        "SUCCESS"
        if not failed and len(completed) == len(quarters)
        else "FAILED"
    )

    manifest = {
        "run_id": run_id,
        "dataset": "fdic_named_silver",
        "start_date": start,
        "end_date": end,
        "quarters_requested": len(quarters),
        "quarters_completed": len(completed),
        "quarters_failed": len(failed),
        "total_named_records": sum(
            item["output_records"]
            for item in completed
        ),
        "completed": completed,
        "failed": failed,
        "status": status,
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    print("\nPUBLISHING SUMMARY")
    print("=" * 50)
    print(json.dumps(manifest, indent=2))

    if status != "SUCCESS":
        raise RuntimeError(
            "Named Silver publishing failed. "
            "Review the failed quarter above."
        )

    # Publish completion marker only after every
    # requested quarter passes validation.
    manifest_key = (
        f"{BUCKET_PREFIX}/named-manifests/"
        f"{start}-{end}/{run_id}.json"
    )

    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print("\nNamed Silver publishing completed")
    print(f"Published manifest: {manifest_key}")

    return manifest


# --------------------------------------------------
# COMMAND-LINE ENTRY POINT
# --------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Publish enriched FDIC Silver financial "
            "data to AWS S3"
        )
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="Destination AWS S3 bucket",
    )

    parser.add_argument(
        "--region",
        default="ca-central-1",
        help="AWS region",
    )

    parser.add_argument(
        "--start",
        required=True,
        help="First reporting quarter: YYYYMMDD",
    )

    parser.add_argument(
        "--end",
        required=True,
        help="Last reporting quarter: YYYYMMDD",
    )

    args = parser.parse_args()

    publish_named_silver(
        bucket=args.bucket,
        region=args.region,
        start=args.start,
        end=args.end,
    )


if __name__ == "__main__":
    main()
