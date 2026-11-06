
import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pyarrow as pa
import pyarrow.parquet as pq


API_URL = "https://api.fdic.gov/banks/financials"

INPUT_ROOT = Path(
    "data/silver/fdic/financials/enriched"
)

OUTPUT_ROOT = Path(
    "data/silver/fdic/financials/named"
)

PAGE_SIZE = 5000


def fetch_page(report_date, offset):
    """Retrieve bank names from one FDIC API page."""

    params = {
        "filters": f"REPDTE:{report_date}",
        "fields": "CERT,NAME,REPDTE",
        "sort_by": "CERT",
        "sort_order": "ASC",
        "limit": PAGE_SIZE,
        "offset": offset,
        "format": "json",
    }

    url = f"{API_URL}?{urlencode(params)}"

    for attempt in range(3):
        try:
            request = Request(
                url,
                headers={"Accept": "application/json"}
            )

            with urlopen(request, timeout=60) as response:
                return json.load(response)

        except (HTTPError, URLError, TimeoutError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def fetch_bank_names(report_date):
    """Download names and validate certificate uniqueness."""

    names = {}
    offset = 0

    while True:
        response = fetch_page(report_date, offset)
        items = response.get("data", [])

        if not isinstance(items, list):
            raise ValueError("Invalid API response")

        if not items:
            break

        for item in items:
            record = item["data"]

            cert = int(record["CERT"])
            name = " ".join(
                str(record.get("NAME") or "").split()
            )

            if cert in names:
                raise ValueError(
                    f"Duplicate financial CERT: {cert}"
                )

            if not name:
                raise ValueError(
                    f"Missing financial bank name: {cert}"
                )

            if str(record["REPDTE"]) != report_date:
                raise ValueError(
                    f"Unexpected report date for CERT {cert}"
                )

            names[cert] = name

        print(
            f"Downloaded {len(items)} records; "
            f"total names: {len(names)}"
        )

        if len(items) < PAGE_SIZE:
            break

        offset += len(items)

    if not names:
        raise ValueError("No bank names returned")

    return names


def enrich_bank_names(report_date):
    """Add reporting-quarter bank names to Silver data."""

    if not re.fullmatch(r"\d{8}", report_date):
        raise ValueError("Expected YYYYMMDD report date")

    input_file = (
        INPUT_ROOT
        / f"report_date={report_date}"
        / "records.parquet"
    )

    if not input_file.exists():
        raise FileNotFoundError(input_file)

    financials = pq.read_table(input_file)

    if "CERT" not in financials.column_names:
        raise ValueError("Missing CERT column")

    certs = financials["CERT"].to_pylist()

    if len(certs) != len(set(certs)):
        raise ValueError("Duplicate CERTs in financial data")

    names_lookup = fetch_bank_names(report_date)

    source_certs = set(certs)
    api_certs = set(names_lookup)

    missing = sorted(source_certs - api_certs)
    unexpected = sorted(api_certs - source_certs)

    # Fail safely if the two datasets disagree.
    if missing or unexpected:
        raise ValueError(
            "Certificate mismatch between Silver and FDIC API. "
            f"Missing: {missing[:10]}; "
            f"Unexpected: {unexpected[:10]}"
        )

    bank_names = [
        names_lookup[cert]
        for cert in certs
    ]

    bank_name_array = pa.array(
        bank_names,
        type=pa.string()
    )

    # Replace lookup-derived names with reporting-date names.
    if "BANK_NAME" in financials.column_names:
        index = financials.schema.get_field_index(
            "BANK_NAME"
        )

        output = financials.set_column(
            index,
            pa.field("BANK_NAME", pa.string()),
            bank_name_array
        )
    else:
        output = financials.append_column(
            "BANK_NAME",
            bank_name_array
        )

    if output.num_rows != financials.num_rows:
        raise ValueError("Financial row count changed")

    output_dir = (
        OUTPUT_ROOT
        / f"report_date={report_date}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "records.parquet"

    pq.write_table(
        output,
        output_file,
        compression="snappy"
    )

    report = {
        "dataset": "fdic_financials_named",
        "report_date": report_date,
        "source_records": financials.num_rows,
        "output_records": output.num_rows,
        "names_matched": len(bank_names),
        "names_missing": 0,
        "name_source": "FDIC Financials API",
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "SUCCESS"
    }

    report_file = output_dir / "quality_report.json"

    report_file.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8"
    )

    print("\nBank-name enrichment completed")
    print(json.dumps(report, indent=2))

    print("\nSample:")
    for cert, name in zip(certs[:5], bank_names[:5]):
        print(f"{cert}: {name}")

    print(f"\nSaved: {output_file}")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        required=True
    )

    args = parser.parse_args()

    enrich_bank_names(args.date)
