
import csv
import io
import json
import sys
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.pipeline import publish_dashboard_history as history


# ==========================================================
# FIXTURES
# ==========================================================

@pytest.fixture
def valid_records():
    """Simulated banking records across two quarters."""

    def make_record(cert, name, date, alerts, priority):
        record = {
            column: ""
            for column in history.COLUMNS
        }

        record.update({
            "CERT": str(cert),
            "BANK_NAME": name,
            "report_date": date,
            "DEP": "100000",
            "ASSET": "150000",
            "LNLSGR": "80000",
            "ROA": "1.25",
            "ROE": "10.50",
            "NCLNLSR": "1.20",
            "RBCRWAJ": "15.00",
            "alert_count": str(alerts),
            "investigation_priority": priority,
        })

        return record

    return [
        make_record(
            1001, "EXAMPLE BANK A",
            "20260331", 0, "NO_ALERT",
        ),
        make_record(
            1002, "EXAMPLE BANK B",
            "20260331", 1, "LOW_REVIEW_PRIORITY",
        ),
        make_record(
            1001, "EXAMPLE BANK A",
            "20260630", 2, "MEDIUM_REVIEW_PRIORITY",
        ),
        make_record(
            1002, "EXAMPLE BANK B",
            "20260630", 3, "HIGH_REVIEW_PRIORITY",
        ),
    ]


@pytest.fixture
def mock_aws(monkeypatch):
    """Mock Athena and S3 so tests use no AWS services."""

    athena = MagicMock()
    s3 = MagicMock()

    def fake_client(service, **kwargs):
        if service == "athena":
            return athena
        if service == "s3":
            return s3

        raise AssertionError(
            f"Unexpected AWS service: {service}"
        )

    monkeypatch.setattr(
        history.boto3,
        "client",
        fake_client,
    )

    # Simulate no historical snapshot being published yet.
    s3.get_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "Object not found",
            }
        },
        "GetObject",
    )

    return athena, s3


# ==========================================================
# DATA VALIDATION TESTS
# ==========================================================

def test_valid_history_passes(valid_records):
    summary = history.validate_history(valid_records)

    assert summary["total_records"] == 4
    assert summary["quarters"] == 2
    assert summary["first_report_date"] == "20260331"
    assert summary["latest_report_date"] == "20260630"

    assert summary["quarter_counts"] == {
        "20260331": 2,
        "20260630": 2,
    }


def test_duplicate_bank_quarter_blocked(valid_records):
    valid_records[1]["CERT"] = "1001"

    with pytest.raises(
        ValueError,
        match="Duplicate bank-quarter",
    ):
        history.validate_history(valid_records)


def test_same_bank_across_quarters_allowed(valid_records):
    summary = history.validate_history(valid_records)

    # The same two banks appear in both quarters.
    assert summary["total_records"] == 4
    assert summary["quarters"] == 2


def test_missing_bank_name_blocked(valid_records):
    valid_records[0]["BANK_NAME"] = ""

    with pytest.raises(
        ValueError,
        match="Missing bank name",
    ):
        history.validate_history(valid_records)


def test_invalid_priority_blocked(valid_records):
    valid_records[0]["investigation_priority"] = (
        "UNKNOWN"
    )

    with pytest.raises(
        ValueError,
        match="Invalid investigation priority",
    ):
        history.validate_history(valid_records)


def test_invalid_alert_count_blocked(valid_records):
    valid_records[0]["alert_count"] = "10"

    with pytest.raises(
        ValueError,
        match="Alert count outside expected range",
    ):
        history.validate_history(valid_records)


def test_csv_contains_expected_columns(valid_records):
    content = history.create_csv(valid_records)

    rows = list(
        csv.DictReader(
            io.StringIO(content.decode("utf-8"))
        )
    )

    assert len(rows) == 4
    assert rows[0]["CERT"] == "1001"
    assert rows[2]["report_date"] == "20260630"
    assert list(rows[0]) == history.COLUMNS


# ==========================================================
# FRESHNESS PROTECTION TESTS
# ==========================================================

def test_older_history_blocked():
    current = {
        "latest_report_date": "20260930"
    }

    with pytest.raises(
        ValueError,
        match="rollback blocked",
    ):
        history.check_freshness("20260630", current)


def test_newer_history_allowed():
    current = {
        "latest_report_date": "20260331"
    }

    history.check_freshness("20260630", current)


# ==========================================================
# AWS SAFETY TESTS
# ==========================================================

def test_plan_never_contacts_aws(monkeypatch):
    def block_aws(*args, **kwargs):
        raise AssertionError(
            "Plan mode must not contact AWS"
        )

    monkeypatch.setattr(
        history.boto3,
        "client",
        block_aws,
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["publish_dashboard_history.py", "--plan"],
    )

    history.main()


def test_mismatched_history_never_uploads(
    monkeypatch,
    valid_records,
    mock_aws,
):
    athena, s3 = mock_aws

    def fake_fetch(client, sql):
        if "COUNT(*)" in sql:
            return [
                {
                    "report_date": "20260331",
                    "total_records": "2",
                    "unique_banks": "2",
                },
                {
                    "report_date": "20260630",
                    "total_records": "3",
                    "unique_banks": "3",
                },
            ]

        return valid_records

    monkeypatch.setattr(
        history, "fetch_rows", fake_fetch
    )

    with pytest.raises(
        ValueError,
        match="quarter counts mismatch",
    ):
        history.publish_history()

    s3.put_object.assert_not_called()


def test_snapshot_uploaded_before_manifest(
    monkeypatch,
    valid_records,
    mock_aws,
):
    athena, s3 = mock_aws

    def fake_fetch(client, sql):
        if "COUNT(*)" in sql:
            return [
                {
                    "report_date": "20260331",
                    "total_records": "2",
                    "unique_banks": "2",
                },
                {
                    "report_date": "20260630",
                    "total_records": "2",
                    "unique_banks": "2",
                },
            ]

        return valid_records

    monkeypatch.setattr(
        history, "fetch_rows", fake_fetch
    )

    uploaded = {}

    def fake_put(**kwargs):
        uploaded[kwargs["Key"]] = kwargs["Body"]

    s3.put_object.side_effect = fake_put

    def fake_head(**kwargs):
        return {
            "ContentLength": len(
                uploaded[kwargs["Key"]]
            )
        }

    s3.head_object.side_effect = fake_head

    manifest = history.publish_history()

    calls = s3.put_object.call_args_list

    assert len(calls) == 2

    snapshot_key = calls[0].kwargs["Key"]
    manifest_key = calls[1].kwargs["Key"]

    assert "/snapshots/" in snapshot_key
    assert manifest_key == history.LATEST_KEY

    pointer = json.loads(uploaded[manifest_key])

    assert pointer["total_records"] == 4
    assert pointer["quarters"] == 2
    assert pointer["snapshot_key"] == snapshot_key
    assert pointer["sha256"] == manifest["sha256"]

    # The publisher checks freshness both
    # before and after uploading the snapshot.
    assert s3.get_object.call_count == 2
