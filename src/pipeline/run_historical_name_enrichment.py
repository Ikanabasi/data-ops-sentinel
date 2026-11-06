
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.transformation.enrich_fdic_financials import enrich_quarter
from src.transformation.add_fdic_financial_names import enrich_bank_names


START_DATE = "20210331"
END_DATE = "20260630"

FINANCIALS_ROOT = Path(
    "data/silver/fdic/financials/parquet"
)

OUTPUT_ROOT = Path(
    "data/silver/fdic/financials/named"
)

BATCH_ROOT = Path(
    "data/silver/fdic/name_enrichment_runs"
)


def generate_quarters(start_date, end_date):
    """Generate quarter-end dates inclusively."""

    def parse_date(value):
        date = datetime.strptime(value, "%Y%m%d")

        if (date.month, date.day) not in {
            (3, 31), (6, 30), (9, 30), (12, 31)
        }:
            raise ValueError(
                f"Not a quarter-end date: {value}"
            )

        return date

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        raise ValueError(
            "Start date cannot be after end date"
        )

    quarters = []

    for year in range(start.year, end.year + 1):
        for month, day in (
            (3, 31), (6, 30), (9, 30), (12, 31)
        ):
            date = datetime(year, month, day)

            if start <= date <= end:
                quarters.append(
                    date.strftime("%Y%m%d")
                )

    return quarters


def run_batch(start_date, end_date, dry_run=False):
    """Enrich all requested reporting quarters."""

    quarters = generate_quarters(
        start_date, end_date
    )

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    completed = []
    failed = []
    total_records = 0

    print("\nFDIC HISTORICAL NAME ENRICHMENT")
    print("=" * 45)
    print(f"Run ID: {run_id}")
    print(f"Quarters requested: {len(quarters)}")
    print(f"Dry run: {dry_run}")

    # Validate all local financial inputs first.
    missing_files = []

    for quarter in quarters:
        source_file = (
            FINANCIALS_ROOT
            / f"report_date={quarter}"
            / "records.parquet"
        )

        if not source_file.exists():
            missing_files.append(str(source_file))

    if missing_files:
        raise FileNotFoundError(
            "Missing financial Parquet inputs:\n"
            + "\n".join(missing_files)
        )

    if dry_run:
        print("\nAll requested financial inputs exist.")

        for quarter in quarters:
            print(f"READY: {quarter}")

        print("\nDry run completed. No files changed.")
        return {
            "status": "DRY_RUN",
            "quarters_requested": len(quarters),
            "quarters_ready": len(quarters),
        }

    for index, quarter in enumerate(
        quarters, start=1
    ):
        print(
            f"\n[{index}/{len(quarters)}] "
            f"Processing {quarter}"
        )

        try:
            # First, add institution metadata.
            institution_report = enrich_quarter(
                quarter
            )

            # Then replace BANK_NAME with the
            # reporting-quarter name from FDIC.
            name_report = enrich_bank_names(
                quarter
            )

            source_count = institution_report[
                "source_records"
            ]

            output_count = name_report[
                "output_records"
            ]

            if source_count != output_count:
                raise ValueError(
                    "Financial row count changed"
                )

            if name_report["names_missing"] != 0:
                raise ValueError(
                    "Missing reporting-quarter bank names"
                )

            if (
                name_report["names_matched"]
                != source_count
            ):
                raise ValueError(
                    "Not all bank names were matched"
                )

            completed.append({
                "report_date": quarter,
                "records": output_count,
                "name_coverage": 100.0,
                "institution_match_rate":
                    institution_report["match_rate"],
            })

            total_records += output_count

            print(
                f"SUCCESS: {quarter} | "
                f"{output_count} named records"
            )

        except Exception as error:
            failed.append({
                "report_date": quarter,
                "error": str(error),
            })

            print(
                f"FAILED: {quarter} | {error}"
            )

    manifest = {
        "run_id": run_id,
        "dataset": "fdic_historical_name_enrichment",
        "start_date": start_date,
        "end_date": end_date,
        "quarters_requested": len(quarters),
        "quarters_completed": len(completed),
        "quarters_failed": len(failed),
        "total_named_records": total_records,
        "completed": completed,
        "failed": failed,
        "status": (
            "SUCCESS" if not failed else "FAILED"
        ),
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    BATCH_ROOT.mkdir(
        parents=True, exist_ok=True
    )

    manifest_file = (
        BATCH_ROOT / f"{run_id}.json"
    )

    manifest_file.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print("\nBATCH SUMMARY")
    print("=" * 45)
    print(
        f"Completed: {len(completed)}/"
        f"{len(quarters)}"
    )
    print(f"Failed: {len(failed)}")
    print(f"Total named records: {total_records}")
    print(f"Status: {manifest['status']}")
    print(f"Manifest: {manifest_file}")

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start", default=START_DATE
    )

    parser.add_argument(
        "--end", default=END_DATE
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    result = run_batch(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    if result["status"] == "FAILED":
        sys.exit(1)
