
import sys
from unittest.mock import MagicMock

import pytest

from src.pipeline import deploy_gold_views as deployment


def test_dry_run_validates_eight_views(monkeypatch):
    """Dry run must not create an AWS client."""

    def block_aws(*args, **kwargs):
        raise AssertionError(
            "Dry run must never contact AWS"
        )

    monkeypatch.setattr(
        deployment.boto3,
        "client",
        block_aws,
    )

    result = deployment.deploy_gold_views(
        dry_run=True
    )

    assert result["status"] == "DRY_RUN_SUCCESS"
    assert result["views_validated"] == 8


def test_deployment_order():
    """All eight Gold views must be deployed in order."""

    views = deployment.prepare_all_views()

    names = [name for name, sql in views]

    assert names == [
        "gold_bank_deposit_trends",
        "gold_bank_deposit_alerts",
        "gold_bank_asset_trends",
        "gold_bank_monitoring",
        "gold_bank_profitability",
        "gold_bank_credit_quality",
        "gold_bank_capital_adequacy",
        "gold_unified_bank_monitoring",
    ]


def test_successful_athena_query():
    """A successful Athena execution returns its ID."""

    client = MagicMock()

    client.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-123"
    }

    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "SUCCEEDED"
            }
        }
    }

    result = deployment.execute_athena_query(
        client,
        "CREATE OR REPLACE VIEW example AS SELECT 1",
        "example",
    )

    assert result == "test-query-123"

    client.start_query_execution.assert_called_once()
    client.get_query_execution.assert_called_once()


def test_failed_athena_query_stops_deployment():
    """Athena failures must raise an exception."""

    client = MagicMock()

    client.start_query_execution.return_value = {
        "QueryExecutionId": "failed-query-123"
    }

    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "FAILED",
                "StateChangeReason": "Simulated SQL error",
            }
        }
    }

    with pytest.raises(
        RuntimeError,
        match="Simulated SQL error",
    ):
        deployment.execute_athena_query(
            client,
            "CREATE OR REPLACE VIEW example AS SELECT 1",
            "example",
        )


def test_all_sql_prepared_before_aws_client(
    monkeypatch,
):
    """Invalid SQL must stop before AWS is contacted."""

    def invalid_sql():
        raise ValueError("Invalid Gold SQL")

    def block_aws(*args, **kwargs):
        raise AssertionError(
            "AWS client must not be created"
        )

    monkeypatch.setattr(
        deployment,
        "prepare_all_views",
        invalid_sql,
    )

    monkeypatch.setattr(
        deployment.boto3,
        "client",
        block_aws,
    )

    with pytest.raises(
        ValueError,
        match="Invalid Gold SQL",
    ):
        deployment.deploy_gold_views(
            dry_run=False
        )


def test_deploy_requires_explicit_approval(
    monkeypatch,
):
    """The command line must reject unapproved deployment."""

    monkeypatch.setattr(
        sys,
        "argv",
        ["deploy_gold_views.py", "--deploy"],
    )

    with pytest.raises(SystemExit) as error:
        deployment.main()

    assert error.value.code == 2


def test_deployment_stops_on_first_failure(
    monkeypatch,
):
    """A failed view must prevent later submissions."""

    fake_views = [
        ("first_view", "SELECT 1"),
        ("second_view", "SELECT 2"),
        ("third_view", "SELECT 3"),
    ]

    monkeypatch.setattr(
        deployment,
        "prepare_all_views",
        lambda: fake_views,
    )

    client = MagicMock()

    monkeypatch.setattr(
        deployment.boto3,
        "client",
        lambda *args, **kwargs: client,
    )

    submitted = []

    def fake_execute(client, sql, view_name):
        submitted.append(view_name)

        if view_name == "second_view":
            raise RuntimeError("Deployment failed")

        return "test-query-id"

    monkeypatch.setattr(
        deployment,
        "execute_athena_query",
        fake_execute,
    )

    with pytest.raises(
        RuntimeError,
        match="Deployment failed",
    ):
        deployment.deploy_gold_views(
            dry_run=False
        )

    assert submitted == [
        "first_view",
        "second_view",
    ]
