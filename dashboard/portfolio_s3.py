"""Read-only, validated portfolio snapshot adapter for the existing S3 loaders.

The existing data_loader.py and history_loader.py continue to validate SHA-256,
required fields, row counts and bank/quarter uniqueness. No AWS keys are used
when both portfolio snapshots exist.
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
    "dashboard/gold/latest.json": ("current", "dashboard/gold/snapshots/"),
    "dashboard/history/latest.json": ("history", "dashboard/history/snapshots/"),
}


class LocalSnapshotClient:
    def get_object(self, *, Bucket, Key):
        if Bucket != BUCKET:
            raise ValueError("Unexpected snapshot bucket")
        if Key in SOURCES:
            kind, _prefix = SOURCES[Key]
            data = (BASE / kind / "latest.json").read_bytes()
        else:
            found = None
            for manifest_key, (kind, prefix) in SOURCES.items():
                manifest_path = BASE / kind / "latest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                if Key == manifest.get("snapshot_key") and Key.startswith(prefix):
                    found = BASE / kind / "snapshot.csv"
                    break
            if found is None:
                raise ValueError(f"Unrecognized snapshot key: {Key}")
            data = found.read_bytes()
        return {"Body": io.BytesIO(data)}


def get_portfolio_or_s3_client(region):
    """Prefer bundled snapshots; opt into S3 locally with DATAOPS_DATA_SOURCE=s3."""
    if os.getenv("DATAOPS_DATA_SOURCE", "").lower() == "s3":
        return boto3.client("s3", region_name=region)
    files = [BASE / kind / file for kind in ("current", "history")
             for file in ("latest.json", "snapshot.csv")]
    if all(path.is_file() for path in files):
        return LocalSnapshotClient()
    return boto3.client("s3", region_name=region)
