
from unittest.mock import MagicMock

import pytest

from src.pipeline import register_athena_partition as reg


QUARTER = "20260630"


def descriptor():
    return {
        "Location": reg.SILVER_ROOT + "/",
        "Columns": [{"Name": "CERT", "Type": "bigint"}],
        "InputFormat": "parquet.input",
        "OutputFormat": "parquet.output",
        "SerdeInfo": {"SerializationLibrary": "parquet.serde"},
    }


def fake_clients(existing=False):
    s3 = MagicMock()
    glue = MagicMock()

    s3.head_object.return_value = {"ContentLength": 100}

    glue.get_table.return_value = {
        "Table": {
            "PartitionKeys": [
                {"Name": "report_date", "Type": "string"}
            ],
            "StorageDescriptor": descriptor(),
        }
    }

    return s3, glue


def test_partition_path():
    assert reg.partition_location(QUARTER).endswith(
        "report_date=20260630"
    )


def test_invalid_quarter():
    with pytest.raises(ValueError):
        reg.partition_location("20260631")


def test_table_metadata_valid():
    _, glue = fake_clients()

    assert reg.verify_table(glue)["Columns"][0]["Name"] == "CERT"


def test_wrong_table_location_blocked():
    _, glue = fake_clients()
    glue.get_table.return_value["Table"][
        "StorageDescriptor"
    ]["Location"] = "s3://unexpected-location"

    with pytest.raises(RuntimeError, match="Unexpected Silver"):
        reg.verify_table(glue)


def test_missing_partition_check_makes_no_changes(monkeypatch):
    s3, glue = fake_clients()

    monkeypatch.setattr(
        reg, "find_partition",
        lambda client, quarter: None,
    )

    result = reg.register_partition(
        s3, glue, QUARTER, execute=False
    )

    assert result["status"] == "REGISTRATION_REQUIRED"
    glue.create_partition.assert_not_called()


def test_existing_partition_is_idempotent(monkeypatch):
    s3, glue = fake_clients()

    monkeypatch.setattr(
        reg,
        "find_partition",
        lambda client, quarter: {
            "StorageDescriptor": {
                "Location": reg.partition_location(quarter)
            }
        },
    )

    result = reg.register_partition(
        s3, glue, QUARTER, execute=True
    )

    assert result["status"] == "REGISTERED"
    glue.create_partition.assert_not_called()


def test_new_partition_is_registered(monkeypatch):
    s3, glue = fake_clients()

    state = {"created": False}

    def lookup(client, quarter):
        if state["created"]:
            return {
                "StorageDescriptor": {
                    "Location": reg.partition_location(quarter)
                }
            }
        return None

    def create(**kwargs):
        state["created"] = True

    monkeypatch.setattr(reg, "find_partition", lookup)
    glue.create_partition.side_effect = create

    result = reg.register_partition(
        s3, glue, QUARTER, execute=True
    )

    assert result["status"] == "REGISTERED"
    glue.create_partition.assert_called_once()


def test_empty_parquet_blocked():
    s3, glue = fake_clients()

    s3.head_object.return_value = {"ContentLength": 0}

    with pytest.raises(RuntimeError, match="empty"):
        reg.register_partition(
            s3, glue, QUARTER, execute=True
        )

    glue.create_partition.assert_not_called()
