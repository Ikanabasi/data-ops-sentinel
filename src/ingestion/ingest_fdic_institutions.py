
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://api.fdic.gov/banks/institutions"

FIELDS = ["CERT", "NAME", "CITY", "STALP", "ACTIVE"]

OUTPUT_ROOT = Path("data/bronze/fdic/institutions")

PAGE_SIZE = 1000


def fetch_page(offset, limit=PAGE_SIZE):
    """Download one page of institution records."""

    params = {
        "fields": ",".join(FIELDS),
        "limit": limit,
        "offset": offset,
        "sort_by": "CERT",
        "sort_order": "ASC",
        "format": "json",
    }

    url = f"{API_URL}?{urlencode(params)}"

    for attempt in range(3):
        try:
            request = Request(
                url,
                headers={"Accept": "application/json"},
            )

            with urlopen(request, timeout=60) as response:
                return json.load(response)

        except (HTTPError, URLError, TimeoutError) as error:
            if attempt == 2:
                raise RuntimeError(
                    f"FDIC request failed at offset {offset}"
                ) from error

            time.sleep(2 ** attempt)


def ingest_institutions(sample=False):
    """Download and validate the FDIC institution directory."""

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    records = []
    seen_certs = set()
    offset = 0
    pages = 0

    while True:
        response = fetch_page(offset)

        items = response.get("data", [])

        if not isinstance(items, list):
            raise ValueError("Invalid FDIC API response")

        if not items:
            break

        for item in items:
            bank = item.get("data")

            if not isinstance(bank, dict):
                raise ValueError("Invalid institution record")

            cert = bank.get("CERT")

            if cert is None or str(cert).strip() == "":
                raise ValueError("Institution missing CERT")

            cert = int(cert)

            if cert in seen_certs:
                raise ValueError(
                    f"Duplicate institution CERT: {cert}"
                )

            seen_certs.add(cert)
            records.append(bank)

        pages += 1

        print(
            f"Page {pages}: {len(items)} records | "
            f"Total collected: {len(records)}"
        )

        if sample:
            break

        if len(items) < PAGE_SIZE:
            break

        offset += len(items)

    if not records:
        raise ValueError("No institution records returned")

    output_dir = OUTPUT_ROOT / (
        "samples" if sample else "snapshots"
    ) / run_id

    output_dir.mkdir(parents=True, exist_ok=True)

    data_file = output_dir / "institutions.json"
    manifest_file = output_dir / "manifest.json"

    with data_file.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2)

    manifest = {
        "run_id": run_id,
        "dataset": "fdic_institutions",
        "status": "SAMPLE" if sample else "SUCCESS",
        "records_collected": len(records),
        "unique_certs": len(seen_certs),
        "pages": pages,
        "fields": FIELDS,
        "source": API_URL,
        "collected_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    with manifest_file.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

    print("\nInstitution ingestion completed")
    print(json.dumps(manifest, indent=2))
    print(f"\nSaved data: {data_file}")

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sample",
        action="store_true",
        help="Download only the first page for testing",
    )

    args = parser.parse_args()

    ingest_institutions(sample=args.sample)
