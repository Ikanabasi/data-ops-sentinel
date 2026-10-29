
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://api.fdic.gov/banks/financials"

TEST_FILTERS = [
    "REPDTE:20260630",
    "REPDTE:2026-06-30",
    "REPDTE:[20260630 TO 20260630]",
]


def test_filter(filter_value):
    params = {
        "filters": filter_value,
        "fields": "CERT,REPDTE,ASSET,DEP,ROA",
        "sort_by": "CERT",
        "sort_order": "ASC",
        "limit": 2,
        "format": "json",
    }

    url = API_URL + "?" + urlencode(params)

    request = Request(
        url,
        headers={"Accept": "application/json"}
    )

    with urlopen(request, timeout=45) as response:
        result = json.load(response)

    records = result.get("data", [])
    total = result.get("meta", {}).get("total")

    print(f"\nFilter: {filter_value}")
    print(f"Matching records: {total}")

    for item in records:
        bank = item["data"]
        print("CERT:", bank.get("CERT"))
        print("Report date:", bank.get("REPDTE"))


for filter_value in TEST_FILTERS:
    try:
        test_filter(filter_value)
    except Exception as error:
        print(f"\nFailed: {filter_value}")
        print("Error:", error)
