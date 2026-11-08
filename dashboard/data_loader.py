
"""
DataOps Sentinel — Current Dashboard Data Loader

Supports:
1. Validated local portfolio snapshots for free hosting.
2. Private AWS S3 snapshots when explicitly selected.

Preserves manifest validation, SHA-256 verification,
required columns and bank-record uniqueness checks.
"""

import hashlib
import io
import json
import re

import pandas as pd
import streamlit as st

try:
    from dashboard.portfolio_s3 import get_portfolio_or_s3_client
except ModuleNotFoundError:
    from portfolio_s3 import get_portfolio_or_s3_client


BUCKET = "dataops-sentinel-lake-ikana-2026"
REGION = "ca-central-1"

LATEST_KEY = "dashboard/gold/latest.json"
SNAPSHOT_PREFIX = "dashboard/gold/snapshots/"


REQUIRED_COLUMNS = {
    "CERT",
    "BANK_NAME",
    "report_date",
    "deposit_change_pct",
    "asset_change_pct",
    "current_roa",
    "current_roe",
    "credit_change_pp",
    "capital_change_pp",
    "alert_count",
    "has_data_limitation",
    "investigation_priority",
}


def get_s3_client():
    """
    Select the configured storage source.

    Portfolio mode:
        Read bundled local snapshots.

    AWS mode:
        Read private S3 snapshots using local
        AWS credentials.

    No AWS credentials are stored in this file.
    """
    return get_portfolio_or_s3_client(REGION)


@st.cache_data(ttl=300, max_entries=1)
def get_latest_manifest():
    """
    Read and validate the latest dashboard manifest.

    Cached for five minutes to reduce repeated reads.
    """

    client = get_s3_client()

    response = client.get_object(
        Bucket=BUCKET,
        Key=LATEST_KEY,
    )

    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    required_fields = {
        "report_date",
        "published_at",
        "snapshot_key",
        "sha256",
        "total_records",
    }

    if not required_fields.issubset(manifest):
        missing = required_fields - set(manifest)

        raise ValueError(
            f"Dashboard manifest missing fields: "
            f"{sorted(missing)}"
        )

    report_date = str(manifest["report_date"])

    if not re.fullmatch(r"\d{8}", report_date):
        raise ValueError(
            "Invalid dashboard reporting date"
        )

    snapshot_key = manifest["snapshot_key"]

    if (
        not isinstance(snapshot_key, str)
        or not snapshot_key.startswith(SNAPSHOT_PREFIX)
        or ".." in snapshot_key
        or not snapshot_key.endswith(".csv")
    ):
        raise ValueError(
            "Invalid dashboard snapshot key"
        )

    expected_hash = manifest["sha256"]

    if (
        not isinstance(expected_hash, str)
        or not re.fullmatch(
            r"[a-fA-F0-9]{64}",
            expected_hash,
        )
    ):
        raise ValueError(
            "Invalid dashboard SHA-256 checksum"
        )

    total_records = manifest["total_records"]

    if (
        type(total_records) is not int
        or total_records <= 0
    ):
        raise ValueError(
            "Invalid dashboard record count"
        )

    return manifest


@st.cache_data(ttl=3600, max_entries=3)
def load_snapshot(snapshot_key, expected_hash):
    """
    Load a versioned current-quarter snapshot.

    Verify the file checksum before loading it.
    """

    client = get_s3_client()

    response = client.get_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    csv_bytes = response["Body"].read()

    actual_hash = hashlib.sha256(
        csv_bytes
    ).hexdigest()

    if actual_hash.lower() != expected_hash.lower():
        raise ValueError(
            "Dashboard snapshot SHA-256 verification failed"
        )

    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        dtype={
            "CERT": str,
            "report_date": str,
        },
    )

    missing_columns = (
        REQUIRED_COLUMNS - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Missing dashboard data columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError(
            "Dashboard snapshot contains no records"
        )

    if df["CERT"].isna().any():
        raise ValueError(
            "Dashboard contains missing bank certificates"
        )

    if df["CERT"].duplicated().any():
        raise ValueError(
            "Dashboard contains duplicate bank certificates"
        )

    if df["BANK_NAME"].isna().any():
        raise ValueError(
            "Dashboard contains missing bank names"
        )

    if (
        df["BANK_NAME"]
        .astype(str)
        .str.strip()
        .eq("")
        .any()
    ):
        raise ValueError(
            "Dashboard contains empty bank names"
        )

    df["alert_count"] = pd.to_numeric(
        df["alert_count"],
        errors="raise",
    )

    if (
        df["alert_count"].isna().any()
        or not df["alert_count"].between(0, 5).all()
    ):
        raise ValueError(
            "Invalid dashboard alert counts"
        )

    df["has_data_limitation"] = (
        df["has_data_limitation"]
        .astype(str)
        .str.lower()
        .map({
            "true": True,
            "false": False,
        })
    )

    if df["has_data_limitation"].isna().any():
        raise ValueError(
            "Invalid dashboard data limitation values"
        )

    return df


def get_dashboard_data():
    """
    Return the current validated dataset and manifest.

    The function signature is unchanged, so the
    existing Streamlit app continues to work.
    """

    manifest = get_latest_manifest()

    df = load_snapshot(
        manifest["snapshot_key"],
        manifest["sha256"],
    )

    if len(df) != manifest["total_records"]:
        raise ValueError(
            "Dashboard record count does not match manifest"
        )

    if not df["report_date"].eq(
        str(manifest["report_date"])
    ).all():
        raise ValueError(
            "Dashboard contains an unexpected reporting quarter"
        )

    return df, manifest
