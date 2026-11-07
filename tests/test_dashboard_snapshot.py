
import csv
import io
import json
import sys
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.pipeline import publish_dashboard_snapshot as publisher


# ==========================================================
# TEST FIXTURES
# ==========================================================

@pytest.fixture
def valid_records():
    """
    Create two simulated banks with valid monitoring data.

    These records are used for testing only.
    They are not retrieved from AWS.
    """

    def make_record(cert, name, alerts, priority):
        record = {
            column: ""
            for column in publisher.COLUMNS
        }

        record.update({
            "CERT": str(cert),
            "BANK_NAME": name,
            "report_date": "20260630",
            "alert_count": str(alerts),
            "has_data_limitation": "false",
            "investigation_priority": priority,
        })

        return record

    return [
        make_record(
            1001,
            "EXAMPLE BANK A",
            0,
            "NO_ALERT",
        ),
        make_record(
            1002,
            "EXAMPLE BANK B",
            3,
            "HIGH_REVIEW_PRIORITY",
        ),
    ]


@pytest.fixture
def mock_aws_clients(monkeypatch):
    """
    Replace AWS clients with simulated clients.

    Simulate a missing latest.json file, as expected
    when publishing a dashboard snapshot for the
    first time.
    """

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
        publisher.boto3,
        "client",
        fake_client,
    )

    s3.get_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": (
                    "The specified key does not exist"
                ),
            }
        },
        "GetObject",
    )

    return athena, s3


# ==========================================================
# TEST 1 — VALID SNAPSHOT
# ==========================================================

def test_valid_snapshot_passes(valid_records):
    """
    A valid snapshot must pass all validation checks.
    """

    publisher.validate_records(
        valid_records,
        "20260630",
        2,
    )


# ==========================================================
# TEST 2 — DUPLICATE BANK CERTIFICATES
# ==========================================================

def test_duplicate_bank_blocked(valid_records):
    """
    Two records cannot have the same CERT in one quarter.
    """

    valid_records[1]["CERT"] = "1001"

    with pytest.raises(
        ValueError,
        match="duplicate CERT",
    ):
        publisher.validate_records(
            valid_records,
            "20260630",
            2,
        )


# ==========================================================
# TEST 3 — INCOMPLETE SNAPSHOT
# ==========================================================

def test_incomplete_snapshot_blocked(valid_records):
    """
    The extracted Gold record count must match
    the expected Silver record count.
    """

    with pytest.raises(
        ValueError,
        match="Row mismatch",
    ):
        publisher.validate_records(
            valid_records,
            "20260630",
            4313,
        )


# ==========================================================
# TEST 4 — MIXED REPORTING QUARTERS
# ==========================================================

def test_mixed_quarters_blocked(valid_records):
    """
    A single snapshot cannot mix two reporting quarters.
    """

    valid_records[1]["report_date"] = "20260331"

    with pytest.raises(
        ValueError,
        match="Mixed reporting",
    ):
        publisher.validate_records(
            valid_records,
            "20260630",
            2,
        )


# ==========================================================
# TEST 5 — INCORRECT INVESTIGATION PRIORITY
# ==========================================================

def test_incorrect_priority_blocked(valid_records):
    """
    Investigation priorities must agree with alert counts.
    """

    valid_records[1][
        "investigation_priority"
    ] = "NO_ALERT"

    with pytest.raises(
        ValueError,
        match="Invalid priority",
    ):
        publisher.validate_records(
            valid_records,
            "20260630",
            2,
        )


# ==========================================================
# TEST 6 — MISSING BANK NAME
# ==========================================================

def test_missing_bank_name_blocked(valid_records):
    """
    Every record must contain an institution name.
    """

    valid_records[0]["BANK_NAME"] = ""

    with pytest.raises(
        ValueError,
        match="Missing bank name",
    ):
        publisher.validate_records(
            valid_records,
            "20260630",
            2,
        )


# ==========================================================
# TEST 7 — CSV SERIALIZATION
# ==========================================================

def test_csv_serialization(valid_records):
    """
    Snapshot serialization must preserve records,
    column names, and their order.
    """

    content = publisher.create_csv(
        valid_records
    )

    assert isinstance(content, bytes)

    reader = csv.DictReader(
        io.StringIO(
            content.decode("utf-8")
        )
    )

    rows = list(reader)

    assert len(rows) == 2

    assert rows[0]["CERT"] == "1001"
    assert rows[1]["CERT"] == "1002"

    assert list(rows[0].keys()) == (
        publisher.COLUMNS
    )


# ==========================================================
# TEST 8 — SAFE PLAN MODE
# ==========================================================

def test_plan_does_not_contact_aws(monkeypatch):
    """
    --plan must never create an AWS client.
    """

    def block_aws(*args, **kwargs):
        raise AssertionError(
            "Plan mode must not contact AWS"
        )

    monkeypatch.setattr(
        publisher.boto3,
        "client",
        block_aws,
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publish_dashboard_snapshot.py",
            "--plan",
        ],
    )

    publisher.main()


# ==========================================================
# TEST 9 — INVALID SNAPSHOT NEVER UPLOADS
# ==========================================================

def test_invalid_snapshot_never_uploads(
    monkeypatch,
    valid_records,
    mock_aws_clients,
):
    """
    Failed data validation must prevent S3 publishing.

    The S3 client simulates a missing latest.json
    manifest rather than returning a MagicMock.
    """

    athena, s3 = mock_aws_clients

    def fake_fetch(client, sql):
        if "COUNT(*)" in sql:
            return [
                {
                    "report_date": "20260630",
                    "total_records": "2",
                    "unique_banks": "2",
                }
            ]

        # Simulate an incomplete Gold result.
        return valid_records[:1]

    monkeypatch.setattr(
        publisher,
        "fetch_rows",
        fake_fetch,
    )

    with pytest.raises(
        ValueError,
        match="Row mismatch",
    ):
        publisher.publish_snapshot()

    # No snapshot or pointer should be uploaded.
    s3.put_object.assert_not_called()


# ==========================================================
# TEST 10 — LATEST POINTER PUBLISHED LAST
# ==========================================================

def test_latest_pointer_published_last(
    monkeypatch,
    valid_records,
    mock_aws_clients,
):
    """
    The versioned CSV must be uploaded before latest.json.

    This prevents an invalid or missing CSV from becoming
    the current dashboard dataset.
    """

    athena, s3 = mock_aws_clients

    def fake_fetch(client, sql):
        if "COUNT(*)" in sql:
            return [
                {
                    "report_date": "20260630",
                    "total_records": "2",
                    "unique_banks": "2",
                }
            ]

        return valid_records

    monkeypatch.setattr(
        publisher,
        "fetch_rows",
        fake_fetch,
    )

    uploaded = {}

    def capture_put(**kwargs):
        uploaded[
            kwargs["Key"]
        ] = kwargs["Body"]

    s3.put_object.side_effect = capture_put

    def fake_head(**kwargs):
        return {
            "ContentLength": len(
                uploaded[kwargs["Key"]]
            )
        }

    s3.head_object.side_effect = fake_head

    manifest = publisher.publish_snapshot()

    calls = s3.put_object.call_args_list

    assert len(calls) == 2

    snapshot_key = (
        calls[0].kwargs["Key"]
    )

    pointer_key = (
        calls[1].kwargs["Key"]
    )

    # The CSV must be uploaded first.
    assert "/snapshots/" in snapshot_key

    # latest.json must be uploaded second.
    assert pointer_key == publisher.LATEST_KEY

    pointer = json.loads(
        uploaded[pointer_key]
    )

    assert (
        pointer["snapshot_key"]
        == snapshot_key
    )

    assert (
        pointer["report_date"]
        == "20260630"
    )

    assert pointer["total_records"] == 2

    assert (
        manifest["sha256"]
        == pointer["sha256"]
    )

    # Verify the publisher checked latest.json
    # before and after preparing the snapshot.
    assert s3.get_object.call_count == 2


# ==========================================================
# TEST 11 — BLOCK OLDER SNAPSHOTS
# ==========================================================

def test_older_snapshot_is_blocked():
    """
    Never replace a newer quarter with older data.
    """

    current_manifest = {
        "report_date": "20260930"
    }

    with pytest.raises(
        ValueError,
        match="Snapshot rollback blocked",
    ):
        publisher.validate_snapshot_freshness(
            "20260630",
            current_manifest,
        )


# ==========================================================
# TEST 12 — ALLOW SAME QUARTER REFRESH
# ==========================================================

def test_same_quarter_can_be_republished():
    """
    Allow corrections or updates to an existing quarter.
    """

    current_manifest = {
        "report_date": "20260630"
    }

    publisher.validate_snapshot_freshness(
        "20260630",
        current_manifest,
    )


# ==========================================================
# TEST 13 — ALLOW NEWER QUARTERS
# ==========================================================

def test_newer_quarter_is_allowed():
    """
    Allow the dashboard to advance when new FDIC
    quarterly data becomes available.
    """

    current_manifest = {
        "report_date": "20260630"
    }

    publisher.validate_snapshot_freshness(
        "20260930",
        current_manifest,
    )
