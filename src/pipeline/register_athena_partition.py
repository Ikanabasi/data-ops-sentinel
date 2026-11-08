
import argparse

import boto3
from botocore.exceptions import ClientError

from src.pipeline.check_fdic_new_quarter import validate_quarter
from src.pipeline.check_gold_readiness import (
    BUCKET,
    DATABASE,
    REGION,
    TABLE,
    get_parquet_key,
)


SILVER_ROOT = (
    f"s3://{BUCKET}/silver/fdic/financials/named"
)


def partition_location(quarter):
    quarter = validate_quarter(quarter)
    return f"{SILVER_ROOT}/report_date={quarter}"


def is_missing(error):
    return (
        error.response.get("Error", {}).get("Code")
        == "EntityNotFoundException"
    )


def verify_table(glue):
    """Verify the table is the expected partitioned Silver table."""

    table = glue.get_table(
        DatabaseName=DATABASE,
        Name=TABLE,
    )["Table"]

    keys = [
        item["Name"]
        for item in table.get("PartitionKeys", [])
    ]

    if keys != ["report_date"]:
        raise RuntimeError(
            f"Unexpected partition keys: {keys}"
        )

    descriptor = table["StorageDescriptor"]
    table_root = descriptor.get("Location", "").rstrip("/")

    if table_root != SILVER_ROOT:
        raise RuntimeError(
            f"Unexpected Silver table location: {table_root}"
        )

    if not descriptor.get("Columns"):
        raise RuntimeError(
            "Silver table has no data columns"
        )

    if not descriptor.get("InputFormat"):
        raise RuntimeError(
            "Silver table is missing its InputFormat"
        )

    if not descriptor.get("OutputFormat"):
        raise RuntimeError(
            "Silver table is missing its OutputFormat"
        )

    if not descriptor.get("SerdeInfo"):
        raise RuntimeError(
            "Silver table is missing its SerDe"
        )

    return descriptor


def find_partition(glue, quarter):
    """Return the registered partition or None."""

    try:
        result = glue.get_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionValues=[quarter],
        )
        return result["Partition"]

    except ClientError as error:
        if is_missing(error):
            return None
        raise


def verify_existing(partition, quarter):
    """Refuse a partition pointing to an unexpected S3 path."""

    actual = partition["StorageDescriptor"].get(
        "Location", ""
    ).rstrip("/")

    expected = partition_location(quarter)

    if actual != expected:
        raise RuntimeError(
            f"Partition location mismatch: {actual}"
        )

    return {
        "quarter": quarter,
        "location": actual,
        "status": "REGISTERED",
    }


def build_partition_input(descriptor, quarter):
    """Copy the approved table storage format and change location."""

    fields = (
        "Columns",
        "InputFormat",
        "OutputFormat",
        "SerdeInfo",
        "Compressed",
        "NumberOfBuckets",
        "StoredAsSubDirectories",
        "Parameters",
    )

    storage = {
        key: descriptor[key]
        for key in fields
        if key in descriptor
    }

    storage["Location"] = partition_location(quarter)

    return {
        "Values": [quarter],
        "StorageDescriptor": storage,
    }


def register_partition(s3, glue, quarter, execute=False):
    quarter = validate_quarter(quarter)

    # Confirm the exact physical data object is available.
    object_metadata = s3.head_object(
        Bucket=BUCKET,
        Key=get_parquet_key(quarter),
    )

    if object_metadata["ContentLength"] <= 0:
        raise RuntimeError(
            "Cannot register an empty Silver Parquet file"
        )

    descriptor = verify_table(glue)

    existing = find_partition(glue, quarter)

    if existing is not None:
        result = verify_existing(existing, quarter)
        print("Partition already registered.")
        return result

    if not execute:
        print(f"Partition missing: {quarter}")
        print("No changes made.")
        return {
            "quarter": quarter,
            "status": "REGISTRATION_REQUIRED",
        }

    print(f"Registering partition: {quarter}")

    try:
        glue.create_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionInput=build_partition_input(
                descriptor, quarter
            ),
        )

    except ClientError as error:
        # Another workflow could have registered it first.
        code = error.response.get("Error", {}).get("Code")

        if code != "AlreadyExistsException":
            raise

    registered = find_partition(glue, quarter)

    if registered is None:
        raise RuntimeError(
            "Partition creation was not confirmed"
        )

    result = verify_existing(registered, quarter)

    print(f"Partition verified: {quarter}")
    return result


def main():
    parser = argparse.ArgumentParser()

    modes = parser.add_mutually_exclusive_group(required=True)

    modes.add_argument("--plan", action="store_true")
    modes.add_argument("--check", metavar="YYYYMMDD")
    modes.add_argument("--register", metavar="YYYYMMDD")

    parser.add_argument("--yes", action="store_true")

    args = parser.parse_args()

    if args.plan:
        print("DATAOPS SENTINEL — PARTITION REGISTRATION")
        print(f"Database: {DATABASE}")
        print(f"Table: {TABLE}")
        print(f"Expected S3 root: {SILVER_ROOT}")
        print("No AWS requests made.")
        return

    if args.register and not args.yes:
        parser.error("--register requires --yes")

    quarter = args.register or args.check

    s3 = boto3.client("s3", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    result = register_partition(
        s3,
        glue,
        quarter,
        execute=bool(args.register),
    )

    print(f"Status: {result['status']}")
    print(f"Quarter: {result['quarter']}")

    # Read-only check must fail when registration is missing.
    if args.check and result["status"] != "REGISTERED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
