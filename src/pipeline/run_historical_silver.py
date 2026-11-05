
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.historical_backfill import (
    generate_quarters,
)
from src.ingestion.manifest_checks import (
    validate_manifest,
)
from src.transformation.transform_fdic_silver import (
    find_bronze_run,
    transform_quarter,
)
from src.pipeline.run_silver import check_quality
from src.storage.export_silver_parquet import (
    export_parquet,
)


ROOT = Path(__file__).resolve().parents[2]

SILVER_ROOT = (
    ROOT / "data" / "silver" / "fdic" / "financials"
)

BATCH_RUNS = (
    ROOT / "data" / "silver" / "fdic" / "batch_runs"
)


def upload_quarter(bucket, region, quarter, parquet_path,
                   quality_report, run_id):
    import boto3

    s3 = boto3.client("s3", region_name=region)

    base = "silver/fdic/financials"

    parquet_key = (
        f"{base}/parquet/"
        f"report_date={quarter}/records.parquet"
    )

    quality_key = (
        f"{base}/quality-reports/"
        f"{quarter}/quality_report.json"
    )

    manifest_key = (
        f"{base}/manifests/"
        f"{quarter}/{run_id}.json"
    )

    # Upload the data and quality report first.
    s3.upload_file(
        str(parquet_path),
        bucket,
        parquet_key,
    )

    s3.upload_file(
        str(quality_report),
        bucket,
        quality_key,
    )

    # Publish the completion marker last.
    marker = {
        "status": "PUBLISHED",
        "report_date": quarter,
        "batch_run_id": run_id,
        "parquet_key": parquet_key,
        "quality_report_key": quality_key,
        "published_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(marker).encode("utf-8"),
        ContentType="application/json",
    )

    return marker


def run_historical_silver(
    start_date="20210331",
    end_date="20260630",
    max_quarters=None,
    dry_run=False,
    publish=False,
    bucket=None,
    region="ca-central-1",
):
    if publish and not bucket:
        raise ValueError(
            "Bucket is required when publishing to S3"
        )

    if max_quarters is not None and max_quarters < 1:
        raise ValueError(
            "max_quarters must be positive"
        )

    quarters = generate_quarters(start_date, end_date)

    if max_quarters is not None:
        quarters = quarters[:max_quarters]

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    summary = {
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "quarters_requested": len(quarters),
        "dry_run": dry_run,
        "publish_requested": publish,
        "completed": [],
        "published": [],
        "planned": [],
        "failed": [],
        "status": "RUNNING",
    }

    try:
        for quarter in quarters:
            print(f"\nProcessing quarter: {quarter}")

            # Verify that Bronze has a success manifest.
            run_dir, bronze_manifest = find_bronze_run(
                quarter
            )

            validate_manifest(bronze_manifest)

            if dry_run:
                print("DRY RUN: Would transform quarter")
                summary["planned"].append(quarter)
                continue

            # 1. Bronze -> Silver CSV + quality report
            report = transform_quarter(quarter)

            # 2. Enforce our strict quality gate
            check_quality(report)

            if (
                report["source_run_id"]
                != bronze_manifest["run_id"]
            ):
                raise RuntimeError(
                    "Bronze source run mismatch"
                )

            # 3. Silver CSV -> Parquet
            parquet_path = export_parquet(quarter)

            quality_path = (
                SILVER_ROOT
                / quarter
                / "quality_report.json"
            )

            if not parquet_path.is_file():
                raise FileNotFoundError(parquet_path)

            if not quality_path.is_file():
                raise FileNotFoundError(quality_path)

            summary["completed"].append(quarter)

            print(
                f"Silver complete: {quarter}, "
                f"{report['accepted_records']} records"
            )

            # 4. Upload only if explicitly requested
            if publish:
                upload_quarter(
                    bucket=bucket,
                    region=region,
                    quarter=quarter,
                    parquet_path=parquet_path,
                    quality_report=quality_path,
                    run_id=run_id,
                )

                summary["published"].append(quarter)
                print(f"S3 published: {quarter}")

        summary["status"] = (
            "DRY_RUN" if dry_run else "SUCCESS"
        )

    except Exception as error:
        summary["status"] = "FAILED"
        summary["failed"].append({
            "quarter": locals().get("quarter"),
            "error_type": type(error).__name__,
            "error_message": str(error),
        })

        raise

    finally:
        summary["finished_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        BATCH_RUNS.mkdir(
            parents=True, exist_ok=True
        )

        summary_path = (
            BATCH_RUNS / f"{run_id}.json"
        )

        with summary_path.open(
            "x", encoding="utf-8"
        ) as file:
            json.dump(summary, file, indent=2)

        print("\nHistorical Silver summary:")
        print(json.dumps(summary, indent=2))
        print(f"Summary saved: {summary_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="FDIC historical Silver batch pipeline"
    )

    parser.add_argument(
        "--start", default="20210331"
    )

    parser.add_argument(
        "--end", default="20260630"
    )

    parser.add_argument(
        "--max-quarters", type=int
    )

    parser.add_argument(
        "--dry-run", action="store_true"
    )

    parser.add_argument(
        "--publish", action="store_true"
    )

    parser.add_argument("--bucket")

    parser.add_argument(
        "--region", default="ca-central-1"
    )

    args = parser.parse_args()

    if args.publish and not args.bucket:
        parser.error(
            "--publish requires --bucket"
        )

    run_historical_silver(
        start_date=args.start,
        end_date=args.end,
        max_quarters=args.max_quarters,
        dry_run=args.dry_run,
        publish=args.publish,
        bucket=args.bucket,
        region=args.region,
    )


if __name__ == "__main__":
    main()
