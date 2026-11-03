
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.ingestion.quarter_dates import validate_quarter_date

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data" / "bronze" / "fdic"

API_URL = "https://api.fdic.gov/banks/financials"

PAGE_SIZE = 500
MAX_RETRIES = 3

FIELDS = [
    "CERT",
    "REPDTE",
    "ASSET",
    "DEP",
    "LNLSGR",
    "EQ",
    "NETINC",
    "ROA",
    "ROE",
    "NIMY",
    "NCLNLSR",
    "RBCRWAJ",
]


# --------------------------------------------------
# FETCH ONE PAGE FROM FDIC
# --------------------------------------------------

def fetch_page(offset, report_date):

    params = {
        "filters": f"REPDTE:{report_date}",
        "fields": ",".join(FIELDS),
        "sort_by": "CERT",
        "sort_order": "ASC",
        "limit": PAGE_SIZE,
        "offset": offset,
        "format": "json",
    }

    url = API_URL + "?" + urlencode(params)

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "DataOpsSentinel/1.0",
        },
    )

    for attempt in range(MAX_RETRIES):

        try:
            with urlopen(request, timeout=60) as response:
                return json.load(response)

        except HTTPError as error:

            if error.code not in (429, 500, 502, 503, 504):
                raise

            if attempt == MAX_RETRIES - 1:
                raise

            print(
                f"HTTP {error.code}. "
                f"Retry {attempt + 1}/{MAX_RETRIES}"
            )

        except (URLError, TimeoutError) as error:

            if attempt == MAX_RETRIES - 1:
                raise

            print(f"Connection error: {error}")

        time.sleep(2 ** attempt)


# --------------------------------------------------
# INGEST A COMPLETE REPORTING QUARTER
# --------------------------------------------------

def ingest_quarter(report_date):

    validate_quarter_date(report_date)

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    run_dir = (
        BRONZE
        / "financials"
        / report_date
        / run_id
    )

    run_dir.mkdir(parents=True, exist_ok=False)

    expected_total = None
    collected = 0
    offset = 0
    page_count = 0
    seen = set()

    manifest = {
        "source": "FDIC",
        "dataset": "financials",
        "report_date": report_date,
        "run_id": run_id,
        "started_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": "RUNNING",
    }

    try:

        while True:

            response = fetch_page(offset, report_date)

            records = response.get("data")

            if not isinstance(records, list):
                raise ValueError(
                    "FDIC response missing data list"
                )

            metadata = response.get("meta")

            if not isinstance(metadata, dict):
                raise ValueError(
                    "FDIC response missing metadata"
                )

            current_total = int(metadata["total"])

            if expected_total is None:
                expected_total = current_total
                print(
                    f"FDIC expects {expected_total} records"
                )

            elif current_total != expected_total:
                raise RuntimeError(
                    "Source record count changed during ingestion"
                )

            if not records:
                break

            if len(records) > PAGE_SIZE:
                raise RuntimeError(
                    "API returned more than the page limit"
                )

            # Validate every record before accepting the page.
            page_keys = set()

            for item in records:

                if not isinstance(item, dict):
                    raise ValueError(
                        "Invalid FDIC record wrapper"
                    )

                record = item.get("data")

                if not isinstance(record, dict):
                    raise ValueError(
                        "Invalid financial record structure"
                    )

                cert = record.get("CERT")
                record_date = str(
                    record.get("REPDTE", "")
                )

                if (
                    not isinstance(cert, int)
                    or cert <= 0
                    or record_date != report_date
                ):
                    raise ValueError(
                        f"Unexpected financial record: {record}"
                    )

                key = (cert, record_date)

                if key in seen or key in page_keys:
                    raise ValueError(
                        f"Duplicate bank-quarter: {key}"
                    )

                page_keys.add(key)

            # Save the original API response.
            page_path = (
                run_dir / f"page_{offset:06d}.json"
            )

            with page_path.open(
                "x",
                encoding="utf-8"
            ) as file:
                json.dump(response, file, indent=2)

            # Only update our counters after saving.
            seen.update(page_keys)

            collected += len(records)
            offset += len(records)
            page_count += 1

            print(
                f"Page {page_count}: "
                f"{collected}/{expected_total} collected"
            )

            if collected >= expected_total:
                break

            if len(records) < PAGE_SIZE:
                raise RuntimeError(
                    "Incomplete page before expected total"
                )

            time.sleep(0.5)

        # Final reconciliation checks.
        if collected != expected_total:
            raise RuntimeError(
                f"Reconciliation failed: "
                f"expected {expected_total}, "
                f"collected {collected}"
            )

        if len(seen) != collected:
            raise RuntimeError(
                "Unique bank-quarter count mismatch"
            )

        if collected == 0:
            raise RuntimeError(
                "No financial records collected"
            )

        manifest.update({
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "expected_records": expected_total,
            "collected_records": collected,
            "unique_keys": len(seen),
            "pages": page_count,
            "status": "SUCCESS",
        })

        print("\nFDIC quarterly ingestion completed!")

    except Exception as error:

        manifest.update({
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "expected_records": expected_total,
            "collected_records": collected,
            "unique_keys": len(seen),
            "pages": page_count,
            "status": "FAILED",
            "error_type": type(error).__name__,
            "error_message": str(error),
        })

        print(f"\nFDIC ingestion failed: {error}")

        raise

    finally:

        manifest_path = run_dir / "manifest.json"

        with manifest_path.open(
            "x",
            encoding="utf-8"
        ) as file:
            json.dump(manifest, file, indent=2)

        print("\nIngestion manifest:")
        print(json.dumps(manifest, indent=2))

    return manifest


# --------------------------------------------------
# COMMAND-LINE ENTRY POINT
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Ingest FDIC quarterly financial data"
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Quarter-end date in YYYYMMDD format",
    )

    args = parser.parse_args()

    try:
        report_date = validate_quarter_date(
            args.date
        )

    except ValueError as error:
        parser.error(str(error))

    ingest_quarter(report_date)


if __name__ == "__main__":
    main()
