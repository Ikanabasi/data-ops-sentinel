
import argparse
from pathlib import Path

import boto3

from src.ingestion.quarter_dates import validate_quarter_date


ROOT = Path(__file__).resolve().parents[2]


def upload_silver_quarter(bucket, report_date, region):
    validate_quarter_date(report_date)

    local_dir = (
        ROOT / "data" / "silver"
        / "fdic" / "financials" / report_date
    )

    filenames = [
        "records.csv",
        "quality_report.json",
    ]

    # Verify all required files exist before uploading.
    for filename in filenames:
        path = local_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Missing Silver file: {path}"
            )

    s3 = boto3.client("s3", region_name=region)

    for filename in filenames:
        local_path = local_dir / filename

        object_key = (
            f"silver/fdic/financials/"
            f"{report_date}/{filename}"
        )

        s3.upload_file(
            str(local_path),
            bucket,
            object_key
        )

        print(
            f"Uploaded: s3://{bucket}/{object_key}"
        )

    print("\nSilver upload completed!")


def main():
    parser = argparse.ArgumentParser(
        description="Upload validated FDIC Silver data to AWS"
    )

    parser.add_argument("--bucket", required=True)
    parser.add_argument("--date", default="20260630")
    parser.add_argument("--region", default="ca-central-1")

    args = parser.parse_args()

    upload_silver_quarter(
        bucket=args.bucket,
        report_date=args.date,
        region=args.region,
    )


if __name__ == "__main__":
    main()
