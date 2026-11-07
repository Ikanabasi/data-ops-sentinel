
import pytest

from src.pipeline.check_fdic_new_quarter import (
    validate_quarter,
    determine_ingestion,
)


def test_valid_quarter():
    assert validate_quarter("20260630") == "20260630"


def test_invalid_quarter_date():
    with pytest.raises(ValueError):
        validate_quarter("20260631")


def test_non_quarter_end_blocked():
    with pytest.raises(ValueError):
        validate_quarter("20260731")


def test_no_ingestion_when_dates_match():
    result = determine_ingestion(
        "20260630",
        "20260630",
    )

    assert result["ingestion_required"] is False


def test_new_quarter_requires_ingestion():
    result = determine_ingestion(
        "20260930",
        "20260630",
    )

    assert result["ingestion_required"] is True


def test_older_fdic_date_is_rejected():
    with pytest.raises(
        ValueError,
        match="older than",
    ):
        determine_ingestion(
            "20260331",
            "20260630",
        )
