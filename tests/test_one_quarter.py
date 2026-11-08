
import subprocess

import pytest

from src.pipeline import run_one_quarter as pipeline


def test_builds_three_pipeline_stages():
    commands = pipeline.build_commands("20260930")

    assert len(commands) == 3

    assert "src.ingestion.historical_backfill" in commands[0]
    assert "src.pipeline.run_historical_silver" in commands[1]
    assert "src.pipeline.publish_named_silver" in commands[2]


def test_invalid_quarter_blocked():
    with pytest.raises(ValueError):
        pipeline.build_commands("20260631")


def test_stages_run_in_order(monkeypatch):
    submitted = []

    def fake_run(command, check):
        assert check is True
        submitted.append(command)

    monkeypatch.setattr(
        pipeline.subprocess,
        "run",
        fake_run,
    )

    commands = pipeline.build_commands("20260930")

    pipeline.run_commands(commands)

    assert submitted == commands


def test_failed_stage_stops_processing(monkeypatch):
    submitted = []

    def fake_run(command, check):
        submitted.append(command)

        if len(submitted) == 2:
            raise subprocess.CalledProcessError(
                1, command
            )

    monkeypatch.setattr(
        pipeline.subprocess,
        "run",
        fake_run,
    )

    with pytest.raises(subprocess.CalledProcessError):
        pipeline.run_commands(
            pipeline.build_commands("20260930")
        )

    assert len(submitted) == 2


def test_no_new_quarter_does_nothing(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "check_live_availability",
        lambda: {
            "status": "UP_TO_DATE",
            "ingestion_required": False,
            "next_quarter": None,
        },
    )

    result = pipeline.run_pipeline()

    assert result["status"] == "UP_TO_DATE"


def test_plan_never_runs_ingestion(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "check_live_availability",
        lambda: {
            "status": "READY_FOR_REVIEW",
            "ingestion_required": True,
            "next_quarter": "20260930",
        },
    )

    def block_run(*args, **kwargs):
        raise AssertionError(
            "Plan must not execute ingestion"
        )

    monkeypatch.setattr(
        pipeline,
        "run_commands",
        block_run,
    )

    result = pipeline.run_pipeline(execute=False)

    assert result["status"] == "PLAN_READY"
    assert result["quarter"] == "20260930"


def test_execution_requires_approval(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "check_live_availability",
        lambda: {
            "status": "READY_FOR_REVIEW",
            "ingestion_required": True,
            "next_quarter": "20260930",
        },
    )

    with pytest.raises(ValueError, match="approval"):
        pipeline.run_pipeline(
            execute=True,
            approved=False,
        )
