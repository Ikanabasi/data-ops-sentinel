
import pytest

from src.pipeline.plan_quarterly_ingestion import (
    next_quarter,
    evaluate_ingestion_plan,
)


def test_next_quarter_after_june():
    assert next_quarter("20260630") == "20260930"


def test_next_quarter_after_december():
    assert next_quarter("20261231") == "20270331"


def test_no_ingestion_when_current():
    result = evaluate_ingestion_plan(
        "20260630",
        "20260630",
    )

    assert result["ingestion_required"] is False
    assert result["next_quarter"] is None
    assert result["status"] == "UP_TO_DATE"


def test_single_new_quarter():
    result = evaluate_ingestion_plan(
        "20260930",
        "20260630",
    )

    assert result["ingestion_required"] is True
    assert result["next_quarter"] == "20260930"


def test_multiple_missing_quarters():
    result = evaluate_ingestion_plan(
        "20270331",
        "20260630",
    )

    assert result["next_quarter"] == "20260930"


def test_older_fdic_date_is_blocked():
    with pytest.raises(
        ValueError,
        match="older than Silver",
    ):
        evaluate_ingestion_plan(
            "20260331",
            "20260630",
        )


def test_invalid_date_is_blocked():
    with pytest.raises(ValueError):
        next_quarter("20260631")
