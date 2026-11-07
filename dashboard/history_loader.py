
import hashlib
import io
import json
import re

import boto3
import pandas as pd
import streamlit as st


BUCKET = "dataops-sentinel-lake-ikana-2026"
REGION = "ca-central-1"

HISTORY_MANIFEST = "dashboard/history/latest.json"
HISTORY_PREFIX = "dashboard/history/snapshots/"

REQUIRED_COLUMNS = {
    "CERT",
    "BANK_NAME",
    "report_date",
    "DEP",
    "ASSET",
    "LNLSGR",
    "ROA",
    "ROE",
    "NCLNLSR",
    "RBCRWAJ",
    "deposit_change_pct",
    "asset_change_pct",
    "roa_change_pp",
    "credit_change_pp",
    "capital_change_pp",
    "alert_count",
    "investigation_priority",
}

NUMERIC_COLUMNS = [
    "DEP",
    "ASSET",
    "LNLSGR",
    "ROA",
    "ROE",
    "NCLNLSR",
    "RBCRWAJ",
    "deposit_change_pct",
    "asset_change_pct",
    "roa_change_pp",
    "credit_change_pp",
    "capital_change_pp",
    "alert_count",
]


def get_s3_client():
    """Use the existing AWS CLI or environment credentials."""
    return boto3.client(
        "s3",
        region_name=REGION,
    )


@st.cache_data(ttl=300, max_entries=1)
def get_history_manifest():
    """Read the latest historical manifest every 5 minutes."""

    s3 = get_s3_client()

    response = s3.get_object(
        Bucket=BUCKET,
        Key=HISTORY_MANIFEST,
    )

    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    required = {
        "snapshot_key",
        "sha256",
        "total_records",
        "quarter_counts",
        "quarters",
        "latest_report_date",
        "published_at",
    }

    if not required.issubset(manifest):
        raise ValueError(
            "Historical manifest is missing required fields"
        )

    key = manifest["snapshot_key"]
    checksum = manifest["sha256"]

    if (
        not isinstance(key, str)
        or not key.startswith(HISTORY_PREFIX)
        or ".." in key
        or not key.endswith(".csv")
    ):
        raise ValueError("Invalid historical snapshot key")

    if (
        not isinstance(checksum, str)
        or not re.fullmatch(r"[a-fA-F0-9]{64}", checksum)
    ):
        raise ValueError("Invalid historical checksum")

    if (
        type(manifest["total_records"]) is not int
        or manifest["total_records"] <= 0
    ):
        raise ValueError("Invalid history record count")

    if not isinstance(manifest["quarter_counts"], dict):
        raise ValueError("Invalid quarter counts")

    return manifest


@st.cache_data(ttl=3600, max_entries=2)
def load_history_snapshot(snapshot_key, checksum):
    """
    Download and validate a versioned historical snapshot.

    The cache key changes when a new snapshot is published.
    """

    s3 = get_s3_client()

    response = s3.get_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    content = response["Body"].read()

    actual_checksum = hashlib.sha256(
        content
    ).hexdigest()

    if actual_checksum.lower() != checksum.lower():
        raise ValueError(
            "Historical snapshot checksum mismatch"
        )

    df = pd.read_csv(
        io.BytesIO(content),
        dtype={
            "CERT": str,
            "report_date": str,
        },
    )

    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Historical columns missing: {sorted(missing)}"
        )

    if df["CERT"].isna().any():
        raise ValueError("Missing bank certificate")

    if df["BANK_NAME"].isna().any():
        raise ValueError("Missing bank name")

    if df["BANK_NAME"].astype(str).str.strip().eq("").any():
        raise ValueError("Empty bank name")

    if df["report_date"].isna().any():
        raise ValueError("Missing report date")

    if df.duplicated(
        subset=["CERT", "report_date"]
    ).any():
        raise ValueError(
            "Duplicate bank-quarter in historical data"
        )

    if not df["report_date"].str.fullmatch(
        r"\d{8}"
    ).all():
        raise ValueError("Invalid historical report date")

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    if (
        df["alert_count"].isna().any()
        or not df["alert_count"].between(0, 5).all()
    ):
        raise ValueError("Invalid historical alert counts")

    df["quarter_date"] = pd.to_datetime(
        df["report_date"],
        format="%Y%m%d",
        errors="raise",
    )

    df["quarter_label"] = (
        df["quarter_date"].dt.year.astype(str)
        + " Q"
        + df["quarter_date"].dt.quarter.astype(str)
    )

    # FDIC balance-sheet amounts are in USD thousands.
    # Convert to USD billions for easier chart reading.
    for column in ["DEP", "ASSET", "LNLSGR"]:
        df[f"{column}_USD_BILLIONS"] = (
            df[column] / 1_000_000
        )

    return df


def get_history_data():
    """Return the latest verified historical dataset."""

    manifest = get_history_manifest()

    df = load_history_snapshot(
        manifest["snapshot_key"],
        manifest["sha256"],
    )

    if len(df) != manifest["total_records"]:
        raise ValueError(
            "Historical record count mismatch"
        )

    actual_counts = {
        str(date): int(count)
        for date, count in df.groupby(
            "report_date"
        ).size().items()
    }

    expected_counts = {
        str(date): int(count)
        for date, count in manifest[
            "quarter_counts"
        ].items()
    }

    if actual_counts != expected_counts:
        raise ValueError(
            "Historical quarter counts do not match manifest"
        )

    if len(actual_counts) != manifest["quarters"]:
        raise ValueError(
            "Historical quarter total mismatch"
        )

    if max(actual_counts) != manifest["latest_report_date"]:
        raise ValueError(
            "Latest historical quarter mismatch"
        )

    return df, manifest
