
import pytest

from src.ingestion.manifest_checks import validate_manifest


def test_successful_ingestion():
    manifest = {
        "expected_records": 4313,
        "collected_records": 4313,
        "unique_keys": 4313,
        "pages": 9,
        "status": "SUCCESS",
    }

    assert validate_manifest(manifest) is True


def test_incomplete_ingestion():
    manifest = {
        "expected_records": 4313,
        "collected_records": 4000,
        "unique_keys": 4000,
        "pages": 8,
        "status": "SUCCESS",
    }

    with pytest.raises(ValueError):
        validate_manifest(manifest)


def test_duplicate_records():
    manifest = {
        "expected_records": 4313,
        "collected_records": 4313,
        "unique_keys": 4300,
        "pages": 9,
        "status": "SUCCESS",
    }

    with pytest.raises(ValueError):
        validate_manifest(manifest)
