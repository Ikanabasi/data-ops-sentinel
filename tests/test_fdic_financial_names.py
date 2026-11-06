
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.transformation import add_fdic_financial_names as module


REPORT_DATE = "20260630"


def make_financial_file(tmp_path, certs):
    """Create a small fake financial dataset for testing."""

    financials_root = tmp_path / "enriched"
    quarter_dir = financials_root / f"report_date={REPORT_DATE}"
    quarter_dir.mkdir(parents=True)

    table = pa.table({
        "CERT": pa.array(certs, type=pa.int64()),
        "BANK_NAME": pa.array(
            ["Old Name"] * len(certs),
            type=pa.string()
        ),
        "ASSET": pa.array(
            [1000] * len(certs),
            type=pa.int64()
        ),
    })

    pq.write_table(
        table,
        quarter_dir / "records.parquet"
    )

    return financials_root


def test_enrichment_preserves_rows_and_adds_names(
    tmp_path, monkeypatch
):
    """Correct matching must preserve all financial rows."""

    input_root = make_financial_file(
        tmp_path, [10, 20]
    )

    output_root = tmp_path / "named"

    monkeypatch.setattr(
        module, "INPUT_ROOT", input_root
    )

    monkeypatch.setattr(
        module, "OUTPUT_ROOT", output_root
    )

    monkeypatch.setattr(
        module,
        "fetch_bank_names",
        lambda date: {
            10: "Example First Bank",
            20: "Example Second Bank",
        },
    )

    report = module.enrich_bank_names(REPORT_DATE)

    output_file = (
        output_root
        / f"report_date={REPORT_DATE}"
        / "records.parquet"
    )

    result = pq.read_table(output_file).to_pylist()

    assert report["status"] == "SUCCESS"
    assert report["source_records"] == 2
    assert report["output_records"] == 2
    assert report["names_matched"] == 2
    assert report["names_missing"] == 0

    assert result[0]["BANK_NAME"] == "Example First Bank"
    assert result[1]["BANK_NAME"] == "Example Second Bank"
    assert result[0]["ASSET"] == 1000


def test_missing_certificate_causes_failure(
    tmp_path, monkeypatch
):
    """Never silently publish an incomplete name lookup."""

    input_root = make_financial_file(
        tmp_path, [10, 20]
    )

    output_root = tmp_path / "named"

    monkeypatch.setattr(
        module, "INPUT_ROOT", input_root
    )

    monkeypatch.setattr(
        module, "OUTPUT_ROOT", output_root
    )

    monkeypatch.setattr(
        module,
        "fetch_bank_names",
        lambda date: {
            10: "Example First Bank"
        },
    )

    with pytest.raises(
        ValueError,
        match="Certificate mismatch"
    ):
        module.enrich_bank_names(REPORT_DATE)

    assert not (
        output_root
        / f"report_date={REPORT_DATE}"
        / "records.parquet"
    ).exists()


def test_duplicate_financial_cert_causes_failure(
    tmp_path, monkeypatch
):
    """Duplicate certificates must not pass validation."""

    input_root = make_financial_file(
        tmp_path, [10, 10]
    )

    monkeypatch.setattr(
        module, "INPUT_ROOT", input_root
    )

    monkeypatch.setattr(
        module,
        "OUTPUT_ROOT",
        tmp_path / "named",
    )

    with pytest.raises(
        ValueError,
        match="Duplicate CERT"
    ):
        module.enrich_bank_names(REPORT_DATE)
