
import pytest

from src.pipeline import run_historical_name_enrichment as pipeline


def test_generates_22_quarters():
    """Check that our date range contains 22 quarters."""

    quarters = pipeline.generate_quarters(
        "20210331", "20260630"
    )

    assert len(quarters) == 22
    assert quarters[0] == "20210331"
    assert quarters[-1] == "20260630"


def test_dry_run_checks_existing_files(tmp_path, monkeypatch):
    """Dry run checks inputs without running enrichment."""

    source_root = tmp_path / "financials"
    quarter_dir = source_root / "report_date=20260630"
    quarter_dir.mkdir(parents=True)

    (quarter_dir / "records.parquet").touch()

    monkeypatch.setattr(
        pipeline, "FINANCIALS_ROOT", source_root
    )

    def must_not_run(*args, **kwargs):
        raise AssertionError(
            "Enrichment ran during dry run"
        )

    monkeypatch.setattr(
        pipeline, "enrich_quarter", must_not_run
    )
    monkeypatch.setattr(
        pipeline, "enrich_bank_names", must_not_run
    )

    result = pipeline.run_batch(
        "20260630",
        "20260630",
        dry_run=True,
    )

    assert result["status"] == "DRY_RUN"
    assert result["quarters_ready"] == 1


def test_missing_input_blocks_batch(tmp_path, monkeypatch):
    """A missing financial file must stop the batch."""

    monkeypatch.setattr(
        pipeline,
        "FINANCIALS_ROOT",
        tmp_path / "missing",
    )

    with pytest.raises(
        FileNotFoundError,
        match="Missing financial Parquet"
    ):
        pipeline.run_batch(
            "20260630",
            "20260630",
            dry_run=True,
        )


def test_incomplete_name_enrichment_fails(
    tmp_path, monkeypatch
):
    """Incomplete name coverage must fail the batch."""

    source_root = tmp_path / "financials"
    quarter_dir = source_root / "report_date=20260630"
    quarter_dir.mkdir(parents=True)
    (quarter_dir / "records.parquet").touch()

    monkeypatch.setattr(
        pipeline, "FINANCIALS_ROOT", source_root
    )
    monkeypatch.setattr(
        pipeline,
        "BATCH_ROOT",
        tmp_path / "batch_runs",
    )

    monkeypatch.setattr(
        pipeline,
        "enrich_quarter",
        lambda date: {
            "source_records": 2,
            "match_rate": 100.0,
        },
    )

    monkeypatch.setattr(
        pipeline,
        "enrich_bank_names",
        lambda date: {
            "output_records": 2,
            "names_matched": 1,
            "names_missing": 1,
        },
    )

    result = pipeline.run_batch(
        "20260630",
        "20260630",
    )

    assert result["status"] == "FAILED"
    assert result["quarters_completed"] == 0
    assert result["quarters_failed"] == 1
