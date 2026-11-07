
import pytest

from src.pipeline.validate_gold_deployment import (
    evaluate_validation,
)


def valid_result(**overrides):
    data = {
        "quarters": 22,
        "silver_records": 102983,
        "gold_records": 102983,
        "failed_quarters": 0,
        "latest_quarter": "20260630",
        "first_quarter": "20210331",
    }

    data.update(overrides)

    return evaluate_validation(**data)


def test_existing_history_passes():
    result = valid_result()

    assert result["status"] == "PASSED"
    assert result["quarters"] == 22


def test_new_quarter_passes():
    result = valid_result(
        quarters=23,
        silver_records=107200,
        gold_records=107200,
        latest_quarter="20260930",
        expected_quarter="20260930",
    )

    assert result["status"] == "PASSED"


def test_missing_gold_records_blocked():
    with pytest.raises(
        RuntimeError,
        match="totals differ",
    ):
        valid_result(
            gold_records=102000,
        )


def test_failed_quarter_blocked():
    with pytest.raises(
        RuntimeError,
        match="failed reconciliation",
    ):
        valid_result(
            failed_quarters=1,
        )


def test_unexpected_latest_quarter_blocked():
    with pytest.raises(
        RuntimeError,
        match="Expected latest quarter",
    ):
        valid_result(
            expected_quarter="20260930",
        )


def test_lost_historical_coverage_blocked():
    with pytest.raises(
        RuntimeError,
        match="at least 22",
    ):
        valid_result(
            quarters=21,
        )
