"""Export existing versioned S3 snapshots for free Streamlit Cloud hosting.

Run locally after `aws login` with read-only S3 credentials.
Checks the SHA-256 checksums before writing files to dashboard/portfolio_data.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import boto3

BUCKET = "dataops-sentinel-lake-ikana-2026"
REGION = "ca-central-1"
ROOT = Path(__file__).resolve().parents[1] / "dashboard" / "portfolio_data"
MANIFESTS = {
    "current": "dashboard/gold/latest.json",
    "history": "dashboard/history/latest.json",
}
PREFIX = {
    "current": "dashboard/gold/snapshots/",
    "history": "dashboard/history/snapshots/",
}


def export_one(s3, kind, manifest_key):
    raw_manifest = s3.get_object(Bucket=BUCKET, Key=manifest_key)["Body"].read()
    manifest = json.loads(raw_manifest)
    key = manifest.get("snapshot_key")
    expected = manifest.get("sha256", "")
    if not isinstance(key, str) or not key.startswith(PREFIX[kind]) or not key.endswith(".csv"):
        raise ValueError(f"Invalid {kind} snapshot key")
    if not isinstance(expected, str) or len(expected) != 64:
        raise ValueError(f"Invalid {kind} checksum")
    csv = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    actual = hashlib.sha256(csv).hexdigest()
    if actual.lower() != expected.lower():
        raise ValueError(f"{kind} snapshot checksum verification failed")
    dest = ROOT / kind
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "latest.json").write_bytes(raw_manifest)
    (dest / "snapshot.csv").write_bytes(csv)
    print(f"{kind}: downloaded {len(csv):,} bytes; SHA-256 verified")


def main():
    s3 = boto3.client("s3", region_name=REGION)
    for kind, key in MANIFESTS.items():
        export_one(s3, kind, key)
    print("Portfolio snapshots ready in dashboard/portfolio_data/")
    print("Review public data and file sizes before committing them to GitHub.")


if __name__ == "__main__":
    main()
