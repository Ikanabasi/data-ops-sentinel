
import argparse

from src.ingestion.quarter_dates import validate_quarter_date
from src.transformation.transform_fdic_silver import transform_quarter
from src.storage.upload_to_s3 import upload_silver_quarter


def check_quality(report):
    source = report["source_records"]
    accepted = report["accepted_records"]
    rejected = report["rejected_records"]

    if report["status"] != "SUCCESS":
        raise RuntimeError(
            "Silver transformation did not succeed"
        )

    if source <= 0:
        raise RuntimeError(
            "Quality gate failed: no source records"
        )

    if accepted + rejected != source:
        raise RuntimeError(
            "Quality gate failed: record counts do not reconcile"
        )

    if rejected > 0:
        raise RuntimeError(
            f"Quality gate failed: {rejected} rejected records"
        )

    print("\nQuality gate PASSED")


def run_pipeline(
    report_date,
    bucket=None,
    region="ca-central-1",
    publish=False,
):
    validate_quarter_date(report_date)

    print(f"\nStarting Silver pipeline: {report_date}")

    # Step 1: Transform local Bronze data.
    report = transform_quarter(report_date)

    # Step 2: Check quality before publishing.
    check_quality(report)

    # Step 3: Upload only when explicitly requested.
    if publish:
        if not bucket:
            raise ValueError(
                "An S3 bucket is required for publishing"
            )

        upload_silver_quarter(
            bucket=bucket,
            report_date=report_date,
            region=region,
        )

        print("\nPipeline completed and published to AWS!")

    else:
        print(
            "\nLocal pipeline completed. "
            "No AWS upload requested."
        )

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Validate and optionally publish FDIC Silver data"
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Quarter-end date in YYYYMMDD format",
    )

    parser.add_argument(
        "--bucket",
        help="Target S3 bucket name",
    )

    parser.add_argument(
        "--region",
        default="ca-central-1",
    )

    parser.add_argument(
        "--publish",
        action="store_true",
        help="Upload passing Silver data to S3",
    )

    args = parser.parse_args()

    if args.publish and not args.bucket:
        parser.error("--bucket is required with --publish")

    run_pipeline(
        report_date=args.date,
        bucket=args.bucket,
        region=args.region,
        publish=args.publish,
    )


if __name__ == "__main__":
    main()
