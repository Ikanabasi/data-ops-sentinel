
"""
DataOps Sentinel — Portfolio Snapshot Storage Adapter

Supports two data sources:

1. Portfolio mode (default):
   Reads validated local snapshots bundled with the app.
   No AWS credentials or S3 requests are required.

2. AWS S3 mode (explicit):
   Set DATAOPS_DATA_SOURCE=s3 to read live private S3 data.

Existing dashboard loaders remain responsible for:
- SHA-256 checksum verification
- Required column validation
- Record count validation
- Bank and quarter uniqueness checks

The public Streamlit app never silently falls back to AWS.
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3


BUCKET = "dataops-sentinel-lake-ikana-2026"

BASE = Path(__file__).resolve().parent / "portfolio_data"

SOURCES = {
    "dashboard/gold/latest.json": (
        "current",
        "dashboard/gold/snapshots/",
    ),
    "dashboard/history/latest.json": (
        "history",
        "dashboard/history/snapshots/",
    ),
}


class LocalSnapshotClient:
    """
    Read bundled snapshot files using the same get_object()
    interface expected by the existing S3 dashboard loaders.
    """

    def get_object(self, *, Bucket, Key):
        if Bucket != BUCKET:
            raise ValueError(
                f"Unexpected snapshot bucket: {Bucket}"
            )

        # Handle the two latest.json manifest files.
        if Key in SOURCES:
            kind, _prefix = SOURCES[Key]

            manifest_path = (
                BASE / kind / "latest.json"
            )

            if not manifest_path.is_file():
                raise FileNotFoundError(
                    f"Portfolio manifest missing: {manifest_path}"
                )

            data = manifest_path.read_bytes()

            return {
                "Body": io.BytesIO(data)
            }

        # Handle versioned CSV snapshot requests.
        for kind, prefix in SOURCES.values():

            manifest_path = (
                BASE / kind / "latest.json"
            )

            snapshot_path = (
                BASE / kind / "snapshot.csv"
            )

            if not manifest_path.is_file():
                raise FileNotFoundError(
                    f"Portfolio manifest missing: {manifest_path}"
                )

            manifest = json.loads(
                manifest_path.read_text(
                    encoding="utf-8"
                )
            )

            expected_key = manifest.get(
                "snapshot_key"
            )

            if (
                Key == expected_key
                and isinstance(Key, str)
                and Key.startswith(prefix)
                and Key.endswith(".csv")
            ):
                if not snapshot_path.is_file():
                    raise FileNotFoundError(
                        f"Portfolio snapshot missing: {snapshot_path}"
                    )

                data = snapshot_path.read_bytes()

                return {
                    "Body": io.BytesIO(data)
                }

        raise ValueError(
            f"Unrecognized portfolio snapshot key: {Key}"
        )


def get_portfolio_or_s3_client(region):
    """
    Select the data source.

    Default:
        Local portfolio snapshots.

    DATAOPS_DATA_SOURCE=s3:
        Explicitly use AWS S3.

    Missing local files raise an understandable error
    instead of silently switching to AWS.
    """

    source = os.getenv(
        "DATAOPS_DATA_SOURCE",
        "portfolio",
    ).strip().lower()

    if source == "s3":
        return boto3.client(
            "s3",
            region_name=region,
        )

    if source != "portfolio":
        raise ValueError(
            "Invalid DATAOPS_DATA_SOURCE value. "
            "Use 'portfolio' or 's3'."
        )

    required_files = [
        BASE / kind / filename
        for kind in ("current", "history")
        for filename in (
            "latest.json",
            "snapshot.csv",
        )
    ]

    missing_files = [
        str(path)
        for path in required_files
        if not path.is_file()
    ]

    if missing_files:
        raise FileNotFoundError(
            "Portfolio snapshot files are missing:\n"
            + "\n".join(missing_files)
            + "\nRun: python -m scripts.export_portfolio_snapshots "
            "on your local computer, then commit the "
            "validated portfolio files."
        )

    return LocalSnapshotClient()
