
import pytest

from src.pipeline import check_gold_readiness as gate


def test_validation_query_contains_all_sources():
    sql = gate.build_validation_query("20260630")

    assert "fdic_financials_named" in sql

    for view in gate.GOLD_VIEWS:
        assert view in sql


def test_invalid_quarter_is_rejected():
    with pytest.raises(ValueError):
        gate.build_validation_query("20260631")


def test_matching_counts_pass():
    sources = [
        gate.TABLE,
        *gate.GOLD_VIEWS,
    ]

    rows = [
        [source, "4313", "4313", "0"]
        for source in sources
    ]

    assert gate.evaluate_source_counts(rows) == 4313


def test_missing_gold_records_fail():
    sources = [
        gate.TABLE,
        *gate.GOLD_VIEWS,
    ]

    rows = [
        [source, "4313", "4313", "0"]
        for source in sources
    ]

    rows[-1][1] = "4300"
    rows[-1][2] = "4300"

    with pytest.raises(
        RuntimeError,
        match="Row count mismatch",
    ):
        gate.evaluate_source_counts(rows)


def test_duplicate_bank_records_fail():
    sources = [
        gate.TABLE,
        *gate.GOLD_VIEWS,
    ]

    rows = [
        [source, "4313", "4313", "0"]
        for source in sources
    ]

    rows[1][2] = "4312"

    with pytest.raises(
        RuntimeError,
        match="Duplicate CERT",
    ):
        gate.evaluate_source_counts(rows)


def test_valid_monitoring_quality_passes():
    gate.evaluate_quality(
        [["4313", "0", "0", "0", "0"]],
        4313,
    )


def test_invalid_monitoring_priority_fails():
    with pytest.raises(
        RuntimeError,
        match="Gold quality validation failed",
    ):
        gate.evaluate_quality(
            [["4313", "0", "0", "2", "0"]],
            4313,
        )


def test_deposit_alerts_can_have_fewer_records():
    """
    Deposit alerts only include banks with
    eligible deposit comparisons.
    """

    sources = [
        gate.TABLE,
        *gate.GOLD_VIEWS,
    ]

    rows = [
        [source, "4313", "4313", "0"]
        for source in sources
    ]

    for row in rows:
        if row[0] == "gold_bank_deposit_alerts":
            row[1] = "4251"
            row[2] = "4251"

    assert gate.evaluate_source_counts(rows) == 4313
