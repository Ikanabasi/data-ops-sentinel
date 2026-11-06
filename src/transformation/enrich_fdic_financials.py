
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


FINANCIALS_ROOT = Path(
    "data/silver/fdic/financials/parquet"
)

INSTITUTIONS_FILE = Path(
    "data/silver/fdic/institutions/institutions.parquet"
)

INSTITUTIONS_REPORT = Path(
    "data/silver/fdic/institutions/quality_report.json"
)

ENRICHED_ROOT = Path(
    "data/silver/fdic/financials/enriched"
)

REPORTS_ROOT = Path(
    "data/silver/fdic/financials/enrichment-reports"
)


def enrich_quarter(report_date):
    """Add bank names and locations to one financial quarter."""

    source_file = (
        FINANCIALS_ROOT
        / f"report_date={report_date}"
        / "records.parquet"
    )

    if not source_file.exists():
        raise FileNotFoundError(
            f"Financial Parquet not found: {source_file}"
        )

    if not INSTITUTIONS_FILE.exists():
        raise FileNotFoundError(
            f"Institution Parquet not found: {INSTITUTIONS_FILE}"
        )

    # Read the financial data and institution lookup.
    financial_table = pq.read_table(source_file)
    institution_table = pq.read_table(INSTITUTIONS_FILE)

    if "CERT" not in financial_table.column_names:
        raise ValueError("Financial data is missing CERT")

    required = {"CERT", "NAME", "CITY", "STALP", "ACTIVE"}

    if not required.issubset(institution_table.column_names):
        raise ValueError("Institution lookup has missing fields")

    # Build a lookup dictionary using CERT.
    institutions = {}

    for bank in institution_table.to_pylist():
        cert = bank["CERT"]

        if cert is None:
            raise ValueError("Institution has missing CERT")

        if cert in institutions:
            raise ValueError(f"Duplicate institution CERT: {cert}")

        institutions[cert] = bank

    # Preserve the original financial record order.
    names = []
    cities = []
    states = []
    active_flags = []

    matched = 0
    unmatched = 0
    unmatched_certs = set()

    for cert in financial_table.column("CERT").to_pylist():
        bank = institutions.get(cert)

        if bank is None:
            unmatched += 1
            unmatched_certs.add(cert)

            names.append(None)
            cities.append(None)
            states.append(None)
            active_flags.append(None)
        else:
            matched += 1

            names.append(bank["NAME"])
            cities.append(bank["CITY"])
            states.append(bank["STALP"])
            active_flags.append(bank["ACTIVE"])

    source_count = financial_table.num_rows

    if matched + unmatched != source_count:
        raise ValueError("Join record count mismatch")

    if source_count > 0 and matched == 0:
        raise ValueError(
            "No financial records matched institution lookup"
        )

    # Append new columns without changing financial values.
    enriched = financial_table

    new_columns = {
        "BANK_NAME": pa.array(names, type=pa.string()),
        "BANK_CITY": pa.array(cities, type=pa.string()),
        "BANK_STATE": pa.array(states, type=pa.string()),
        "BANK_ACTIVE_CURRENT": pa.array(
            active_flags, type=pa.int8()
        ),
    }

    for column_name, column_values in new_columns.items():
        if column_name in enriched.column_names:
            raise ValueError(
                f"Column already exists: {column_name}"
            )

        enriched = enriched.append_column(
            column_name, column_values
        )

    if enriched.num_rows != source_count:
        raise ValueError("Enrichment changed financial row count")

    # Save enriched Parquet separately from original Silver.
    output_dir = (
        ENRICHED_ROOT / f"report_date={report_date}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "records.parquet"

    pq.write_table(
        enriched,
        output_file,
        compression="snappy",
    )

    # Capture the lookup snapshot for data lineage.
    lookup_report = json.loads(
        INSTITUTIONS_REPORT.read_text(encoding="utf-8")
    )

    match_rate = (
        round(100 * matched / source_count, 2)
        if source_count
        else 0.0
    )

    report = {
        "dataset": "fdic_financials_enriched",
        "report_date": report_date,
        "source_records": source_count,
        "enriched_records": enriched.num_rows,
        "matched_records": matched,
        "unmatched_records": unmatched,
        "unique_unmatched_certs": len(unmatched_certs),
        "unmatched_cert_sample": sorted(
            cert for cert in unmatched_certs
            if cert is not None
        )[:20],
        "match_rate": match_rate,
        "institution_snapshot": lookup_report[
            "source_snapshot"
        ],
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "SUCCESS",
    }

    report_dir = REPORTS_ROOT / report_date
    report_dir.mkdir(parents=True, exist_ok=True)

    report_file = report_dir / "quality_report.json"

    report_file.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\nFinancial enrichment completed")
    print(json.dumps(report, indent=2))

    print("\nSample enriched records:")

    for record in enriched.slice(0, 5).to_pylist():
        print({
            "CERT": record["CERT"],
            "BANK_NAME": record["BANK_NAME"],
            "BANK_CITY": record["BANK_CITY"],
            "BANK_STATE": record["BANK_STATE"],
        })

    print(f"\nSaved: {output_file}")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        required=True,
        help="Quarter-end date in YYYYMMDD format",
    )

    args = parser.parse_args()

    enrich_quarter(args.date)
