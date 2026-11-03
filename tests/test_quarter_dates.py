
import pytest

from src.ingestion.quarter_dates import (
    validate_quarter_date
)


@pytest.mark.parametrize(
    "date_value",
    [
        "20260331",
        "20260630",
        "20250930",
    ]
)
def test_valid_quarter_dates(date_value):
    assert validate_quarter_date(date_value) == date_value


@pytest.mark.parametrize(
    "date_value",
    [
        "2026-06-30",
        "20260615",
        "20261331",
    ]
)
def test_invalid_quarter_dates(date_value):
    with pytest.raises(ValueError):
        validate_quarter_date(date_value)
