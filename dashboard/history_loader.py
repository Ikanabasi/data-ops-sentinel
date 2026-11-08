
"""
DataOps Sentinel — Historical Dashboard Data Loader

Supports validated portfolio snapshots and private S3.

Preserves:
- SHA-256 verification
- Historical record counts
- Quarterly reporting counts
- Duplicate bank-quarter detection
- Numeric conversion
- Financial amounts in USD billions for compatibility
  with the existing Streamlit application

The app.py presentation layer converts billions to
millions for the financial charts.
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
    """
    Use the portfolio adapter.

    Bundled data is used when local portfolio
    snapshots are available.

    Live S3 can be selected explicitly using:
        DATAOPS_DATA_SOURCE=s3
    """
    return get_portfolio_or_s3_client(REGION)


@st.cache_data(ttl=300, max_entries=1)
def get_history_manifest():
    """
    Read and validate the historical manifest.

    The manifest is cached for five minutes.
    """

    client = get_s3_client()

    response = client.get_object(
        Bucket=BUCKET,
        Key=HISTORY_MANIFEST,
    )

    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    required_fields = {
        "snapshot_key",
        "sha256",
        "total_records",
        "quarter_counts",
        "quarters",
        "latest_report_date",
        "published_at",
    }

    if not required_fields.issubset(manifest):
        missing = required_fields - set(manifest)

        raise ValueError(
            "Historical manifest missing fields: "
            f"{sorted(missing)}"
        )

    snapshot_key = manifest["snapshot_key"]

    if (
        not isinstance(snapshot_key, str)
        or not snapshot_key.startswith(HISTORY_PREFIX)
        or ".." in snapshot_key
        or not snapshot_key.endswith(".csv")
    ):
        raise ValueError(
            "Invalid historical snapshot key"
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
            "Invalid historical SHA-256 checksum"
        )

    if (
        type(manifest["total_records"]) is not int
        or manifest["total_records"] <= 0
    ):
        raise ValueError(
            "Invalid historical record count"
        )

    if (
        type(manifest["quarters"]) is not int
        or manifest["quarters"] <= 0
    ):
        raise ValueError(
            "Invalid historical quarter total"
        )

    if not isinstance(
        manifest["quarter_counts"], dict
    ):
        raise ValueError(
            "Invalid historical quarter counts"
        )

    if not re.fullmatch(
        r"\d{8}",
        str(manifest["latest_report_date"]),
    ):
        raise ValueError(
            "Invalid latest historical reporting date"
        )

    return manifest


@st.cache_data(ttl=3600, max_entries=2)
def load_history_snapshot(snapshot_key, checksum):
    """
    Read and validate the full historical snapshot.

    Preserves the column names expected by app.py.
    """

    client = get_s3_client()

    response = client.get_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    csv_bytes = response["Body"].read()

    actual_checksum = hashlib.sha256(
        csv_bytes
    ).hexdigest()

    if actual_checksum.lower() != checksum.lower():
        raise ValueError(
            "Historical snapshot SHA-256 checksum mismatch"
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
            "Historical columns missing: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError(
            "Historical snapshot contains no records"
        )

    if df["CERT"].isna().any():
        raise ValueError(
            "Historical data contains missing bank certificates"
        )

    if df["BANK_NAME"].isna().any():
        raise ValueError(
            "Historical data contains missing bank names"
        )

    if (
        df["BANK_NAME"]
        .astype(str)
        .str.strip()
        .eq("")
        .any()
    ):
        raise ValueError(
            "Historical data contains empty bank names"
        )

    if df["report_date"].isna().any():
        raise ValueError(
            "Historical data contains missing reporting dates"
        )

    if df.duplicated(
        subset=["CERT", "report_date"]
    ).any():
        raise ValueError(
            "Duplicate bank-quarter records found"
        )

    if not df["report_date"].str.fullmatch(
        r"\d{8}"
    ).all():
        raise ValueError(
            "Invalid historical reporting date format"
        )

    # Convert financial and monitoring values to numbers.
    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    if (
        df["alert_count"].isna().any()
        or not df["alert_count"].between(0, 5).all()
    ):
        raise ValueError(
            "Invalid historical alert counts"
        )

    # Create real dates for Plotly time-series charts.
    df["quarter_date"] = pd.to_datetime(
        df["report_date"],
        format="%Y%m%d",
        errors="raise",
    )

    # Human-readable labels such as 2026 Q2.
    df["quarter_label"] = (
        df["quarter_date"].dt.year.astype(str)
        + " Q"
        + df["quarter_date"].dt.quarter.astype(str)
    )

    # FDIC monetary values are reported in USD thousands.
    #
    # Keep these BILLIONS columns because the existing
    # app.py uses them. The revised UI multiplies them
    # by 1,000 to display USD millions.
    for column in ["DEP", "ASSET", "LNLSGR"]:
        df[f"{column}_USD_BILLIONS"] = (
            df[column] / 1_000_000
        )

    return df


def get_history_data():
    """
    Return the verified historical dataset and manifest.

    Verify total records, reporting-quarter counts,
    quarter coverage and latest reporting period.
    """

    manifest = get_history_manifest()

    df = load_history_snapshot(
        manifest["snapshot_key"],
        manifest["sha256"],
    )

    if len(df) != manifest["total_records"]:
        raise ValueError(
            "Historical record count does not match manifest"
        )

    actual_counts = {
        str(report_date): int(count)
        for report_date, count in (
            df.groupby("report_date").size().items()
        )
    }

    expected_counts = {
        str(report_date): int(count)
        for report_date, count in (
            manifest["quarter_counts"].items()
        )
    }

    if actual_counts != expected_counts:
        raise ValueError(
            "Historical quarter counts do not match manifest"
        )

    if len(actual_counts) != manifest["quarters"]:
        raise ValueError(
            "Historical quarter total does not match manifest"
        )

    if max(actual_counts) != str(
        manifest["latest_report_date"]
    ):
        raise ValueError(
            "Latest historical quarter does not match manifest"
        )

    return df, manifest
