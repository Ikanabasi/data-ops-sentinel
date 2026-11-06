
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.pipeline import publish_named_silver as publisher


def test_download_financial_parquet(
    tmp_path, monkeypatch
):
    """Check correct S3 source path and local destination."""

    monkeypatch.setattr(
        publisher,
        "FINANCIALS_ROOT",
        tmp_path / "financials",
    )

    s3 = MagicMock()

    def fake_download(bucket, key, filename):
        Path(filename).write_bytes(b"test parquet content")

    s3.download_file.side_effect = fake_download

    result = publisher.download_financial_parquet(
        s3,
        "test-bucket",
        "20260630",
    )

    assert result.exists()
    assert result.stat().st_size > 0

    s3.download_file.assert_called_once_with(
        "test-bucket",
        (
            "silver/fdic/financials/parquet/"
            "report_date=20260630/records.parquet"
        ),
        str(result),
    )


def test_upload_and_verify_success(tmp_path):
    """Correct S3 file size must pass verification."""

    test_file = tmp_path / "records.parquet"
    test_file.write_bytes(b"example file")

    s3 = MagicMock()

    s3.head_object.return_value = {
        "ContentLength": test_file.stat().st_size
    }

    publisher.upload_and_verify(
        s3,
        "test-bucket",
        test_file,
        "silver/fdic/financials/named/test.parquet",
    )

    s3.upload_file.assert_called_once()
    s3.head_object.assert_called_once()


def test_upload_size_mismatch_fails(tmp_path):
    """An incomplete S3 upload must fail validation."""

    test_file = tmp_path / "records.parquet"
    test_file.write_bytes(b"example file")

    s3 = MagicMock()

    s3.head_object.return_value = {
        "ContentLength": 1
    }

    with pytest.raises(
        ValueError,
        match="S3 size mismatch",
    ):
        publisher.upload_and_verify(
            s3,
            "test-bucket",
            test_file,
            "silver/fdic/financials/named/test.parquet",
        )


def test_missing_upload_file_fails(tmp_path):
    """Missing local data must never be uploaded."""

    missing_file = tmp_path / "missing.parquet"

    s3 = MagicMock()

    with pytest.raises(FileNotFoundError):
        publisher.upload_and_verify(
            s3,
            "test-bucket",
            missing_file,
            "silver/fdic/financials/named/test.parquet",
        )

    s3.upload_file.assert_not_called()


def test_empty_upload_file_fails(tmp_path):
    """Empty data must never be published."""

    empty_file = tmp_path / "empty.parquet"
    empty_file.touch()

    s3 = MagicMock()

    with pytest.raises(
        ValueError,
        match="Cannot upload an empty file",
    ):
        publisher.upload_and_verify(
            s3,
            "test-bucket",
            empty_file,
            "silver/fdic/financials/named/test.parquet",
        )

    s3.upload_file.assert_not_called()


def test_invalid_bucket_blocks_pipeline():
    """Missing bucket configuration must fail early."""

    with pytest.raises(
        ValueError,
        match="S3 bucket is required",
    ):
        publisher.publish_named_silver(
            bucket="",
            region="ca-central-1",
            start="20260630",
            end="20260630",
        )
