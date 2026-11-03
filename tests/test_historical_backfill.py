
import json

import pytest

from src.ingestion import historical_backfill as hb


def test_generate_quarters():
    result = hb.generate_quarters(
        "20250331", "20251231"
    )

    assert result == [
        "20250331",
        "20250630",
        "20250930",
        "20251231",
    ]


def test_five_year_window():
    result = hb.generate_quarters(
        "20210331", "20260630"
    )

    assert len(result) == 22
    assert result[0] == "20210331"
    assert result[-1] == "20260630"


def test_invalid_date_range():
    with pytest.raises(ValueError):
        hb.generate_quarters(
            "20260630", "20210331"
        )


def test_existing_successful_run(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        hb, "FINANCIALS_DIR", tmp_path
    )

    run_dir = (
        tmp_path / "20260630" / "run_001"
    )
    run_dir.mkdir(parents=True)

    manifest = {
        "report_date": "20260630",
        "expected_records": 2,
        "collected_records": 2,
        "unique_keys": 2,
        "pages": 1,
        "status": "SUCCESS",
    }

    (run_dir / "manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8"
    )

    (run_dir / "page_000000.json").write_text(
        "{}",
        encoding="utf-8"
    )

    assert hb.find_successful_run(
        "20260630"
    ) == manifest


def test_failed_run_not_skipped(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        hb, "FINANCIALS_DIR", tmp_path
    )

    run_dir = (
        tmp_path / "20260630" / "run_002"
    )
    run_dir.mkdir(parents=True)

    (run_dir / "manifest.json").write_text(
        json.dumps({
            "report_date": "20260630",
            "expected_records": 10,
            "collected_records": 5,
            "unique_keys": 5,
            "pages": 1,
            "status": "FAILED",
        }),
        encoding="utf-8"
    )

    assert hb.find_successful_run(
        "20260630"
    ) is None
