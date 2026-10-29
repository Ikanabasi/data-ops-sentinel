
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = ROOT / "docs" / "schema_discovery"

BASE_URL = "https://api.fdic.gov/banks"

# The fields we want for our banking dashboard.
DATASETS = {
    "institutions": [
        "CERT", "NAME", "CITY",
        "STALP", "ACTIVE", "BKCLASS"
    ],
    "financials": [
        "CERT", "REPDTE", "ASSET", "DEP",
        "LNLSGR", "EQ", "NETINC",
        "ROA", "ROE", "NIM",
        "NCLNLSR", "RBCRWAJ"
    ]
}


def request_json(endpoint, fields):
    params = {
        "fields": ",".join(fields),
        "limit": 1,
        "format": "json"
    }

    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"

    request = Request(
        url,
        headers={
            "User-Agent": "DataOpsSentinel/1.0",
            "Accept": "application/json"
        }
    )

    with urlopen(request, timeout=45) as response:
        return json.load(response)


def discover_dataset(endpoint, fields):
    print(f"\nChecking {endpoint}...")

    try:
        response = request_json(endpoint, fields)

        records = response.get("data", [])

        if not isinstance(records, list) or not records:
            raise ValueError(
                f"No valid records returned by {endpoint}"
            )

        # FDIC wraps each result in a "data" object.
        sample = records[0]["data"]

        if not isinstance(sample, dict):
            raise ValueError("Unexpected record structure")

        field_report = {}

        for field in fields:
            value = sample.get(field)

            field_report[field] = {
                "present": field in sample,
                "sample_type": type(value).__name__,
                "sample_value": value
            }

            print(
                f"{field:12} | "
                f"{type(value).__name__:10} | "
                f"{str(value)[:55]}"
            )

        return {
            "status": "SUCCESS",
            "endpoint": endpoint,
            "fields": field_report,
            "sample_record": sample
        }

    except (HTTPError, URLError, ValueError, KeyError) as error:
        print(f"Discovery failed: {error}")

        return {
            "status": "FAILED",
            "endpoint": endpoint,
            "error": str(error)
        }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = {
        "discovered_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "datasets": {}
    }

    for endpoint, fields in DATASETS.items():
        results["datasets"][endpoint] = (
            discover_dataset(endpoint, fields)
        )

    output_file = OUTPUT_DIR / "fdic_schema_report.json"

    with output_file.open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(results, file, indent=2)

    print(f"\nSchema report saved: {output_file}")

    if any(
        dataset["status"] != "SUCCESS"
        for dataset in results["datasets"].values()
    ):
        raise SystemExit(
            "Some schema checks failed. Review the report."
        )


if __name__ == "__main__":
    main()
