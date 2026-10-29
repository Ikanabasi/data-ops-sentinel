
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://api.fdic.gov/banks/financials"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "fdic"

PAGE_SIZE = 100
MAX_RECORDS = 1000

FIELDS = "CERT,NAME,REPDTE,ASSET,DEP,NETINC,ROA"


def fetch_page(offset, limit):
    params = {
        "fields": FIELDS,
        "sort_by": "REPDTE",
        "sort_order": "DESC",
        "limit": limit,
        "offset": offset,
        "format": "json",
    }

    url = API_URL + "?" + urlencode(params)

    request = Request(
        url,
        headers={
            "User-Agent": "DataOpsSentinel/0.1",
            "Accept": "application/json",
        },
    )

    with urlopen(request, timeout=60) as response:
        return json.load(response)


def run_backfill():
    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    run_dir = BRONZE_DIR / f"backfill_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)

    offset = 0
    total_collected = 0
    expected_count = None

    while total_collected < MAX_RECORDS:
        page_limit = min(
            PAGE_SIZE,
            MAX_RECORDS - total_collected
        )

        print(f"Fetching offset {offset}...")

        response = fetch_page(offset, page_limit)

        records = response.get("data")
        metadata = response.get("meta", {})

        if not isinstance(records, list):
            raise ValueError(
                "FDIC response missing data list"
            )

        if expected_count is None:
            expected_count = int(metadata["total"])

        if not records:
            break

        page_file = run_dir / f"page_{offset:06d}.json"

        with page_file.open("x", encoding="utf-8") as file:
            json.dump(response, file, indent=2)

        total_collected += len(records)
        offset += len(records)

        print(f"Collected: {total_collected}")

        if offset >= expected_count:
            break

        # Avoid sending requests too rapidly.
        time.sleep(0.5)

    target_count = min(expected_count, MAX_RECORDS)

    if total_collected != target_count:
        raise RuntimeError(
            f"Incomplete backfill: expected {target_count}, "
            f"collected {total_collected}"
        )

    print("\nBackfill completed!")
    print(f"Records collected: {total_collected}")
    print(f"API matching records: {expected_count}")
    print(f"Saved in: {run_dir}")


if __name__ == "__main__":
    run_backfill()
