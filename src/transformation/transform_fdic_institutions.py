
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


BRONZE_ROOT = Path("data/bronze/fdic/institutions/snapshots")
SILVER_ROOT = Path("data/silver/fdic/institutions")


def clean_text(value):
    """Remove extra spaces from a string."""
    if value is None:
        return None

    result = " ".join(str(value).split())
    return result if result else None


def clean_institution(record):
    """Validate and standardize one institution."""

    try:
        cert = int(record["CERT"])
    except (KeyError, TypeError, ValueError):
        return None, "INVALID_CERT"

    if cert <= 0:
        return None, "INVALID_CERT"

    name = clean_text(record.get("NAME"))

    if not name:
        return None, "MISSING_NAME"

    active_value = record.get("ACTIVE")

    if str(active_value) not in ("0", "1"):
        return None, "INVALID_ACTIVE_STATUS"

    institution = {
        "CERT": cert,
        "NAME": name,
        "CITY": clean_text(record.get("CITY")),
        "STALP": (
            clean_text(record.get("STALP")) or ""
        ).upper() or None,
        "ACTIVE": int(active_value),
    }

    return institution, None


def transform_institutions():
    """Convert the latest successful Bronze snapshot to Silver."""

    snapshots = sorted(
        BRONZE_ROOT.glob("*/manifest.json"),
        reverse=True,
    )

    source_dir = None

    for manifest_path in snapshots:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if manifest.get("status") == "SUCCESS":
            source_dir = manifest_path.parent
            break

    if source_dir is None:
        raise FileNotFoundError(
            "No successful institution snapshot found"
        )

    raw_file = source_dir / "institutions.json"

    records = json.loads(
        raw_file.read_text(encoding="utf-8")
    )

    clean_records = []
    rejected_records = []
    seen_certs = set()

    for record in records:
        cleaned, reason = clean_institution(record)

        if cleaned is not None:
            if cleaned["CERT"] in seen_certs:
                reason = "DUPLICATE_CERT"
            else:
                seen_certs.add(cleaned["CERT"])

        if reason:
            rejected_records.append({
                "record": record,
                "rejection_reason": reason,
            })
        else:
            clean_records.append(cleaned)

    if not clean_records:
        raise ValueError("No valid institution records")

    schema = pa.schema([
        pa.field("CERT", pa.int64(), nullable=False),
        pa.field("NAME", pa.string(), nullable=False),
        pa.field("CITY", pa.string()),
        pa.field("STALP", pa.string()),
        pa.field("ACTIVE", pa.int8(), nullable=False),
    ])

    table = pa.Table.from_pylist(
        clean_records,
        schema=schema,
    )

    SILVER_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = SILVER_ROOT / "institutions.parquet"

    pq.write_table(
        table,
        parquet_path,
        compression="snappy",
    )

    rejected_path = SILVER_ROOT / "rejected.json"

    rejected_path.write_text(
        json.dumps(rejected_records, indent=2),
        encoding="utf-8",
    )

    source_count = len(records)
    accepted_count = len(clean_records)
    rejected_count = len(rejected_records)

    quality_report = {
        "dataset": "fdic_institutions",
        "source_snapshot": source_dir.name,
        "source_records": source_count,
        "accepted_records": accepted_count,
        "rejected_records": rejected_count,
        "pass_rate": round(
            100 * accepted_count / source_count, 2
        ),
        "active_institutions": sum(
            bank["ACTIVE"] == 1
            for bank in clean_records
        ),
        "inactive_institutions": sum(
            bank["ACTIVE"] == 0
            for bank in clean_records
        ),
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    report_path = SILVER_ROOT / "quality_report.json"

    report_path.write_text(
        json.dumps(quality_report, indent=2),
        encoding="utf-8",
    )

    print("\nInstitution Silver transformation completed")
    print(json.dumps(quality_report, indent=2))
    print(f"\nParquet file: {parquet_path}")

    return quality_report


if __name__ == "__main__":
    transform_institutions()
