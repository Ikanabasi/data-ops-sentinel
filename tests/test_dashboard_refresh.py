
from src.pipeline.check_dashboard_refresh import (
    determine_refresh,
)


def test_no_refresh_when_dates_match():
    result = determine_refresh(
        "20260630",
        "20260630",
        "20260630",
    )

    assert result["refresh_current"] is False
    assert result["refresh_history"] is False


def test_refresh_when_silver_is_newer():
    result = determine_refresh(
        "20260930",
        "20260630",
        "20260630",
    )

    assert result["refresh_current"] is True
    assert result["refresh_history"] is True


def test_only_history_needs_refresh():
    result = determine_refresh(
        "20260930",
        "20260930",
        "20260630",
    )

    assert result["refresh_current"] is False
    assert result["refresh_history"] is True
