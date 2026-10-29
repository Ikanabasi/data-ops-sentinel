
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze" / "fdic"

API_URL = "https://api.fdic.gov/banks/financials"

REPORT_DATE = "20260630"
PAGE_SIZE = 500

FIELDS = [
    "CERT", "REPDTE", "ASSET", "DEP",
    "LNLSGR", "EQ", "NETINC",
    "ROA", "ROE", "NIMY",
    "NCLNLSR", "RBCRWAJ"
]


def fetch_page(offset):
    params = {
        "filters": f"REPDTE:{REPORT_DATE}",
        "fields": ",".join(FIELDS),
        "sort_by": "CERT",
        "sort_order": "ASC",
        "limit": PAGE_SIZE,
        "offset": offset,
        "format": "json"
    }

    url = API_URL + "?" + urlencode(params)

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "DataOpsSentinel/1.0"
        }
    )

    for attempt in range(3):
        try:
            with urlopen(request, timeout=60) as response:
                return json.load(response)

        except HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise

        except (URLError, TimeoutError):
            if attempt == 2:
                raise

        time.sleep(2 ** attempt)


def ingest_quarter():
    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    run_dir = (
        BRONZE / "financials" / REPORT_DATE / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=False)

    expected_total = None
    collected = 0
    offset = 0
    seen = set()
    page_count = 0

    while True:
        response = fetch_page(offset)
        records = response.get("data")

        if not isinstance(records, list):
            raise ValueError("Invalid API response structure")

        current_total = int(response["meta"]["total"])

        if expected_total is None:
            expected_total = current_total
            print(f"FDIC expects {expected_total} records")

        elif current_total != expected_total:
            raise RuntimeError(
                "Source count changed during extraction"
            )

        if not records:
            break

        # Verify bank-quarter identity.
        for item in records:
            record = item["data"]

            cert = record.get("CERT")
            report_date = str(record.get("REPDTE"))

            if not cert or report_date != REPORT_DATE:
                raise ValueError(
                    f"Unexpected record: {record}"
                )

            key = (cert, report_date)

            if key in seen:
                raise ValueError(
                    f"Duplicate bank-quarter: {key}"
                )

            seen.add(key)

        # Preserve each raw response.
        page_path = run_dir / f"page_{offset:06d}.json"

        with page_path.open("x", encoding="utf-8") as file:
            json.dump(response, file, indent=2)

        page_count += 1
        collected += len(records)
        offset += len(records)

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

    if collected != expected_total:
        raise RuntimeError(
            f"Reconciliation failed: "
            f"{collected} != {expected_total}"
        )

    manifest = {
        "source": "FDIC",
        "dataset": "financials",
        "report_date": REPORT_DATE,
        "run_id": run_id,
        "completed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "expected_records": expected_total,
        "collected_records": collected,
        "unique_keys": len(seen),
        "pages": page_count,
        "status": "SUCCESS"
    }

    with (run_dir / "manifest.json").open(
        "x", encoding="utf-8"
    ) as file:
        json.dump(manifest, file, indent=2)

    print("\nQuarterly ingestion completed!")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    ingest_quarter()
