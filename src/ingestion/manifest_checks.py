
def validate_manifest(manifest):
    expected = manifest["expected_records"]
    collected = manifest["collected_records"]
    unique = manifest["unique_keys"]
    pages = manifest["pages"]

    if expected < 1:
        raise ValueError("Invalid expected count")

    if collected != expected:
        raise ValueError("Record count mismatch")

    if unique != collected:
        raise ValueError("Duplicate records detected")

    if pages < 1:
        raise ValueError("No pages recorded")

    if manifest["status"] != "SUCCESS":
        raise ValueError("Ingestion incomplete")

    return True
