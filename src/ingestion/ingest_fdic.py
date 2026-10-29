
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Official FDIC financial data endpoint
API_URL = "https://api.fdic.gov/banks/financials"

# Start with 10 actual bank financial records
PARAMS = {
    "fields": "CERT,REPDTE,ASSET,DEP,NETINC,ROA",
    "sort_by": "REPDTE",
    "sort_order": "DESC",
    "limit": 10,
    "format": "json"
}

# Locate our project root automatically
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Raw FDIC data storage
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "fdic"


def ingest_fdic():
    print("Connecting to FDIC API...")

    # Convert our parameters into a URL query
    url = API_URL + "?" + urlencode(PARAMS)

    request = Request(
        url,
        headers={
            "User-Agent": "DataOpsSentinel/0.1",
            "Accept": "application/json"
        }
    )

    # Contact the API and retrieve JSON data
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)

    # Verify the expected response structure
    records = payload.get("data")

    if not isinstance(records, list):
        raise ValueError("Invalid FDIC API response")

    # Create the Bronze storage directory
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Generate our ingestion timestamp
    ingested_at = datetime.now(timezone.utc)

    filename = ingested_at.strftime(
        "%Y%m%dT%H%M%S%fZ.json"
    )

    # Add useful ingestion metadata
    ingestion_record = {
        "source": "FDIC",
        "ingested_at": ingested_at.isoformat(),
        "request_url": url,
        "record_count": len(records),
        "payload": payload
    }

    # Create our output filename
    output_file = BRONZE_DIR / filename

    # Save the original response with its metadata
    with output_file.open("x", encoding="utf-8") as file:
        json.dump(ingestion_record, file, indent=2)

    print("FDIC ingestion completed!")
    print(f"Records received: {len(records)}")
    print(f"Saved to: {output_file}")

    # Display the first bank's financial fields
    if records:
        first_bank = records[0].get("data", {})

        print("\nSample financial record:")
        for field, value in first_bank.items():
            print(f"{field}: {value}")


if __name__ == "__main__":
    ingest_fdic()
