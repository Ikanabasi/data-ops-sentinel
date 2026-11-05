
import pytest

from src.pipeline import run_silver as pipeline


def good_report():
    return {
        "status": "SUCCESS",
        "source_records": 4313,
        "accepted_records": 4313,
        "rejected_records": 0,
    }


def test_quality_gate_passes():
    pipeline.check_quality(good_report())


def test_quality_gate_rejects_bad_records():
    report = good_report()
    report["accepted_records"] = 4312
    report["rejected_records"] = 1

    with pytest.raises(RuntimeError):
        pipeline.check_quality(report)


def test_quality_gate_detects_count_mismatch():
    report = good_report()
    report["accepted_records"] = 4300

    with pytest.raises(RuntimeError):
        pipeline.check_quality(report)


def test_no_cloud_upload_without_publish(monkeypatch):
    uploads = []

    monkeypatch.setattr(
        pipeline,
        "transform_quarter",
        lambda date: good_report(),
    )

    monkeypatch.setattr(
        pipeline,
        "upload_silver_quarter",
        lambda **kwargs: uploads.append(kwargs),
    )

    pipeline.run_pipeline(
        report_date="20260630",
        publish=False,
    )

    assert uploads == []


def test_cloud_upload_after_quality_pass(monkeypatch):
    uploads = []

    monkeypatch.setattr(
        pipeline,
        "transform_quarter",
        lambda date: good_report(),
    )

    monkeypatch.setattr(
        pipeline,
        "upload_silver_quarter",
        lambda **kwargs: uploads.append(kwargs),
    )

    pipeline.run_pipeline(
        report_date="20260630",
        bucket="test-bucket",
        publish=True,
    )

    assert len(uploads) == 1
    assert uploads[0]["bucket"] == "test-bucket"


def test_cloud_upload_blocked_on_quality_failure(
    monkeypatch,
):
    uploads = []

    bad_report = good_report()
    bad_report["rejected_records"] = 1
    bad_report["accepted_records"] = 4312

    monkeypatch.setattr(
        pipeline,
        "transform_quarter",
        lambda date: bad_report,
    )

    monkeypatch.setattr(
        pipeline,
        "upload_silver_quarter",
        lambda **kwargs: uploads.append(kwargs),
    )

    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(
            report_date="20260630",
            bucket="test-bucket",
            publish=True,
        )

    assert uploads == []
