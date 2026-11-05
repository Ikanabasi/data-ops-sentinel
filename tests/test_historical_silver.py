
from pathlib import Path

import pytest

from src.pipeline import run_historical_silver as hs


def test_publish_requires_bucket():
    with pytest.raises(ValueError):
        hs.run_historical_silver(
            publish=True
        )


def test_invalid_max_quarters():
    with pytest.raises(ValueError):
        hs.run_historical_silver(
            max_quarters=0
        )


def test_dry_run_never_transforms(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        hs, "BATCH_RUNS", tmp_path
    )

    monkeypatch.setattr(
        hs, "generate_quarters",
        lambda start, end: ["20260331"]
    )

    monkeypatch.setattr(
        hs, "find_bronze_run",
        lambda quarter: (
            Path("test-run"),
            {"status": "SUCCESS"}
        )
    )

    monkeypatch.setattr(
        hs, "validate_manifest",
        lambda manifest: None
    )

    def should_not_run(*args, **kwargs):
        raise AssertionError(
            "Dry run must not transform or upload"
        )

    monkeypatch.setattr(
        hs, "transform_quarter",
        should_not_run
    )

    monkeypatch.setattr(
        hs, "upload_quarter",
        should_not_run
    )

    result = hs.run_historical_silver(
        dry_run=True
    )

    assert result["status"] == "DRY_RUN"
    assert result["planned"] == ["20260331"]


def test_quality_failure_blocks_upload(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        hs, "BATCH_RUNS", tmp_path
    )

    monkeypatch.setattr(
        hs, "generate_quarters",
        lambda start, end: ["20260630"]
    )

    manifest = {
        "status": "SUCCESS",
        "run_id": "test-run"
    }

    monkeypatch.setattr(
        hs, "find_bronze_run",
        lambda quarter: (
            Path("test-run"), manifest
        )
    )

    monkeypatch.setattr(
        hs, "validate_manifest",
        lambda manifest: None
    )

    monkeypatch.setattr(
        hs, "transform_quarter",
        lambda quarter: {
            "status": "SUCCESS",
            "source_records": 10,
            "accepted_records": 9,
            "rejected_records": 1,
            "source_run_id": "test-run",
        }
    )

    def should_not_upload(*args, **kwargs):
        raise AssertionError(
            "Bad data must never be published"
        )

    monkeypatch.setattr(
        hs, "upload_quarter",
        should_not_upload
    )

    with pytest.raises(RuntimeError):
        hs.run_historical_silver(
            publish=True,
            bucket="example-bucket",
        )
