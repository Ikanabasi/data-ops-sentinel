
import hashlib
import io
import json
import re

import boto3
import pandas as pd
import streamlit as st


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
    Use AWS credentials from the environment.

    No AWS access keys are stored in this code.
    """
    return boto3.client(
        "s3",
        region_name=REGION,
    )


@st.cache_data(ttl=300, max_entries=1)
def get_latest_manifest():
    """
    Read the latest published manifest.

    Cache for five minutes to avoid requesting
    latest.json on every widget interaction.
    """
    s3 = get_s3_client()

    response = s3.get_object(
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
        raise ValueError(
            "Manifest is missing required fields"
        )

    if not re.fullmatch(
        r"\d{8}",
        str(manifest["report_date"]),
    ):
        raise ValueError(
            "Invalid manifest reporting date"
        )

    snapshot_key = manifest["snapshot_key"]

    if (
        not isinstance(snapshot_key, str)
        or not snapshot_key.startswith(SNAPSHOT_PREFIX)
        or ".." in snapshot_key
        or not snapshot_key.endswith(".csv")
    ):
        raise ValueError(
            "Invalid snapshot object key"
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
            "Invalid SHA-256 checksum"
        )

    if (
        not isinstance(manifest["total_records"], int)
        or isinstance(manifest["total_records"], bool)
        or manifest["total_records"] <= 0
    ):
        raise ValueError(
            "Invalid expected record count"
        )

    return manifest


@st.cache_data(ttl=3600, max_entries=3)
def load_snapshot(snapshot_key, expected_hash):
    """
    Load one versioned snapshot from S3.

    Data is cached by snapshot key and checksum.
    A new checksum causes the new snapshot
    to be loaded automatically.
    """
    s3 = get_s3_client()

    response = s3.get_object(
        Bucket=BUCKET,
        Key=snapshot_key,
    )

    csv_bytes = response["Body"].read()

    actual_hash = hashlib.sha256(
        csv_bytes
    ).hexdigest()

    if actual_hash.lower() != expected_hash.lower():
        raise ValueError(
            "Snapshot SHA-256 verification failed"
        )

    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        dtype={
            "CERT": str,
            "report_date": str,
        },
    )

    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing data columns: {sorted(missing_columns)}"
        )

    if df["CERT"].isna().any():
        raise ValueError(
            "Snapshot has missing bank certificates"
        )

    if df["CERT"].duplicated().any():
        raise ValueError(
            "Snapshot has duplicate bank certificates"
        )

    if df["BANK_NAME"].isna().any():
        raise ValueError(
            "Snapshot has missing bank names"
        )

    df["alert_count"] = pd.to_numeric(
        df["alert_count"],
        errors="raise",
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
            "Invalid data limitation values"
        )

    return df


def get_dashboard_data():
    """
    Retrieve the latest validated dashboard dataset.
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
        manifest["report_date"]
    ).all():
        raise ValueError(
            "Dashboard snapshot contains an unexpected quarter"
        )

    return df, manifest
