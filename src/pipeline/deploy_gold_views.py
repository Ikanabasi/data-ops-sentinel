
import argparse
import re
import time
from pathlib import Path

import boto3
import sqlglot
from sqlglot import exp


GOLD_DIR = Path("sql/gold")

DATABASE = "dataops_sentinel"
WORKGROUP = "dataops-sentinel"
REGION = "ca-central-1"

SQL_FILES = [
    "01_gold_bank_deposit_trends.sql",
    "02_gold_bank_deposit_alerts.sql",
    "03_gold_bank_asset_trends.sql",
    "04_gold_bank_monitoring.sql",
    "05_gold_bank_profitability.sql",
    "06_gold_bank_credit_quality.sql",
    "07_gold_bank_capital_adequacy.sql",
    "08_gold_unified_bank_monitoring.sql",
]


def prepare_sql(filename):
    """Validate and prepare one Gold view definition."""

    path = GOLD_DIR / filename

    if not path.is_file():
        raise FileNotFoundError(
            f"Missing Gold SQL file: {path}"
        )

    sql = path.read_text(
        encoding="utf-8"
    ).strip().rstrip(";").strip()

    if not sql:
        raise ValueError(f"Empty SQL file: {filename}")

    statements = sqlglot.parse(
        sql,
        read="trino",
        error_level="RAISE",
    )

    if len(statements) != 1:
        raise ValueError(
            f"Expected exactly one SQL statement: {filename}"
        )

    statement = statements[0]

    if not isinstance(statement, exp.Create):
        raise ValueError(
            f"Expected CREATE VIEW in {filename}"
        )

    if statement.args.get("kind", "").upper() != "VIEW":
        raise ValueError(
            f"Not a view definition: {filename}"
        )

    expected_name = filename[3:-4]

    if statement.this.name.lower() != expected_name:
        raise ValueError(
            f"View name mismatch in {filename}"
        )

    pattern = (
        r"^CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"
    )

    if not re.match(pattern, sql, flags=re.IGNORECASE):
        raise ValueError(
            f"Unexpected CREATE VIEW syntax: {filename}"
        )

    # Allow repeatable deployment without dropping views.
    sql = re.sub(
        pattern,
        "CREATE OR REPLACE VIEW ",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    return expected_name, sql


def prepare_all_views():
    """Validate every SQL file before any deployment."""

    prepared = []

    for filename in SQL_FILES:
        view_name, sql = prepare_sql(filename)
        prepared.append((view_name, sql))

    return prepared


def execute_athena_query(client, sql, view_name):
    """Run one Athena query and check its final status."""

    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": DATABASE,
            "Catalog": "AwsDataCatalog",
        },
        WorkGroup=WORKGROUP,
    )

    execution_id = response["QueryExecutionId"]

    print(f"Query ID: {execution_id}")

    deadline = time.monotonic() + 180

    while time.monotonic() < deadline:
        result = client.get_query_execution(
            QueryExecutionId=execution_id
        )

        status = result["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            print(f"SUCCESS: {view_name}")
            return execution_id

        if state in ("FAILED", "CANCELLED"):
            reason = status.get(
                "StateChangeReason",
                "No error details returned",
            )

            raise RuntimeError(
                f"Athena deployment failed for "
                f"{view_name}: {reason}"
            )

        time.sleep(2)

    raise TimeoutError(
        f"Athena query did not finish before the "
        f"timeout for {view_name}. "
        f"Check query ID {execution_id} in Athena."
    )


def deploy_gold_views(dry_run=True):
    """Validate or deploy the eight Gold views."""

    views = prepare_all_views()

    print("\nDATAOPS SENTINEL — GOLD DEPLOYMENT")
    print("=" * 45)
    print(f"Database: {DATABASE}")
    print(f"Workgroup: {WORKGROUP}")
    print(f"Views: {len(views)}")
    print(f"Mode: {'DRY RUN' if dry_run else 'DEPLOY'}")

    print("\nDeployment order:")

    for index, (name, _) in enumerate(views, start=1):
        print(f"{index}. {name}")

    if dry_run:
        print("\nDRY RUN PASSED")
        print("All SQL files are valid.")
        print("No AWS requests were made.")
        return {
            "status": "DRY_RUN_SUCCESS",
            "views_validated": len(views),
        }

    client = boto3.client(
        "athena",
        region_name=REGION,
    )

    completed = []

    for name, sql in views:
        print(f"\nDeploying: {name}")

        query_id = execute_athena_query(
            client,
            sql,
            name,
        )

        completed.append({
            "view": name,
            "query_id": query_id,
        })

    print("\nGOLD DEPLOYMENT SUCCESSFUL")
    print(f"Views deployed: {len(completed)}")

    return {
        "status": "SUCCESS",
        "completed": completed,
    }


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate locally without AWS access",
    )

    mode.add_argument(
        "--deploy",
        action="store_true",
        help="Execute Gold SQL in Athena",
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Explicitly approve AWS deployment",
    )

    args = parser.parse_args()

    if args.deploy and not args.yes:
        parser.error(
            "--deploy requires --yes to approve "
            "AWS changes"
        )

    deploy_gold_views(
        dry_run=not args.deploy
    )


if __name__ == "__main__":
    main()
