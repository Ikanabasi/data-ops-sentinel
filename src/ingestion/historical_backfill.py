
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.ingest_fdic_quarter import ingest_quarter
from src.ingestion.quarter_dates import validate_quarter_date
from src.ingestion.manifest_checks import validate_manifest


ROOT = Path(__file__).resolve().parents[2]

FINANCIALS_DIR = (
    ROOT / "data" / "bronze" / "fdic" / "financials"
)

HISTORY_DIR = (
    ROOT / "data" / "bronze" / "fdic" / "history_runs"
)

QUARTER_ENDS = {
    3: 31,
    6: 30,
    9: 30,
    12: 31,
}


def generate_quarters(start_date, end_date):
    validate_quarter_date(start_date)
    validate_quarter_date(end_date)

    if start_date > end_date:
        raise ValueError(
            "Start date must be before end date"
        )

    quarters = []

    for year in range(
        int(start_date[:4]),
        int(end_date[:4]) + 1
    ):
        for month, day in QUARTER_ENDS.items():

            quarter = f"{year}{month:02d}{day:02d}"

            if start_date <= quarter <= end_date:
                quarters.append(quarter)

    return quarters


def find_successful_run(report_date):
    quarter_dir = FINANCIALS_DIR / report_date

    if not quarter_dir.exists():
        return None

    for run_dir in sorted(
        quarter_dir.iterdir(),
        reverse=True
    ):
        if not run_dir.is_dir():
            continue

        manifest_file = run_dir / "manifest.json"

        if not manifest_file.exists():
            continue

        try:
            with manifest_file.open(
                encoding="utf-8"
            ) as file:
                manifest = json.load(file)

            if manifest.get("report_date") != report_date:
                continue

            validate_manifest(manifest)

            saved_pages = list(
                run_dir.glob("page_*.json")
            )

            if len(saved_pages) != manifest["pages"]:
                continue

            return manifest

        except (
            OSError,
            ValueError,
            KeyError,
            TypeError
        ):
            continue

    return None


def run_history(
    start_date,
    end_date,
    dry_run=False,
    max_quarters=None
):
    quarters = generate_quarters(
        start_date, end_date
    )

    if max_quarters is not None:
        if max_quarters < 1:
            raise ValueError(
                "max_quarters must be positive"
            )

        quarters = quarters[:max_quarters]

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    summary = {
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "dry_run": dry_run,
        "quarters_requested": len(quarters),
        "completed": [],
        "skipped": [],
        "would_ingest": [],
        "failed": [],
    }

    for quarter in quarters:
        print(f"\nProcessing {quarter}...")

        existing = find_successful_run(quarter)

        if existing:
            print("Already completed. Skipping.")
            summary["skipped"].append(quarter)
            continue

        if dry_run:
            print("DRY RUN: Would ingest this quarter.")
            summary["would_ingest"].append(quarter)
            continue

        try:
            manifest = ingest_quarter(quarter)

            validate_manifest(manifest)

            summary["completed"].append(quarter)

        except Exception as error:
            print(f"Failed: {error}")

            summary["failed"].append({
                "quarter": quarter,
                "error": str(error),
            })

            # Stop rather than silently continue
            # after an unexpected source failure.
            break

    summary["status"] = (
        "FAILED"
        if summary["failed"]
        else "DRY_RUN" if dry_run
        else "SUCCESS"
    )

    summary["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()

    HISTORY_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    summary_file = HISTORY_DIR / f"{run_id}.json"

    with summary_file.open(
        "x", encoding="utf-8"
    ) as file:
        json.dump(summary, file, indent=2)

    print("\nHistorical ingestion summary:")
    print(json.dumps(summary, indent=2))

    if summary["failed"]:
        raise RuntimeError(
            "Historical backfill failed. "
            f"See {summary_file}"
        )

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="FDIC historical financial backfill"
    )

    parser.add_argument(
        "--start",
        default="20210331"
    )

    parser.add_argument(
        "--end",
        default="20260630"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true"
    )

    parser.add_argument(
        "--max-quarters",
        type=int,
        default=None
    )

    args = parser.parse_args()

    try:
        run_history(
            start_date=args.start,
            end_date=args.end,
            dry_run=args.dry_run,
            max_quarters=args.max_quarters
        )

    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
