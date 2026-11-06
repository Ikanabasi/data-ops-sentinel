
import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp


GOLD_DIR = Path("sql/gold")

GOLD_FILES = [
    "01_gold_bank_deposit_trends.sql",
    "02_gold_bank_deposit_alerts.sql",
    "03_gold_bank_asset_trends.sql",
    "04_gold_bank_monitoring.sql",
    "05_gold_bank_profitability.sql",
    "06_gold_bank_credit_quality.sql",
    "07_gold_bank_capital_adequacy.sql",
    "08_gold_unified_bank_monitoring.sql",
]

# Dependencies must be created before their consumers.
DEPENDENCIES = {
    "gold_bank_deposit_trends": [],
    "gold_bank_deposit_alerts": [
        "gold_bank_deposit_trends",
    ],
    "gold_bank_asset_trends": [],
    "gold_bank_monitoring": [
        "gold_bank_deposit_trends",
        "gold_bank_deposit_alerts",
        "gold_bank_asset_trends",
    ],
    "gold_bank_profitability": [],
    "gold_bank_credit_quality": [],
    "gold_bank_capital_adequacy": [],
    "gold_unified_bank_monitoring": [
        "gold_bank_monitoring",
        "gold_bank_profitability",
        "gold_bank_credit_quality",
        "gold_bank_capital_adequacy",
    ],
}


def read_sql(filename):
    path = GOLD_DIR / filename
    return path.read_text(encoding="utf-8").strip()


def view_name(filename):
    return filename.removesuffix(".sql")[3:]


def referenced_gold_views(sql):
    """Find Gold views used in FROM and JOIN clauses."""

    matches = re.findall(
        r"\b(?:FROM|JOIN)\s+"
        r"(?:dataops_sentinel\.)?"
        r"(gold_[a-zA-Z0-9_]+)\b",
        sql,
        flags=re.IGNORECASE,
    )

    return set(name.lower() for name in matches)


def test_all_gold_sql_files_exist():
    """All eight SQL definitions must exist."""

    assert GOLD_DIR.is_dir()

    actual_files = {
        path.name for path in GOLD_DIR.glob("*.sql")
    }

    assert actual_files == set(GOLD_FILES)


@pytest.mark.parametrize("filename", GOLD_FILES)
def test_gold_sql_is_not_empty(filename):
    """SQL files must contain actual statements."""

    sql = read_sql(filename)

    assert len(sql) > 20
    assert "CREATE" in sql.upper()
    assert "VIEW" in sql.upper()


@pytest.mark.parametrize("filename", GOLD_FILES)
def test_gold_sql_parses(filename):
    """SQLGlot must parse each Athena view."""

    sql = read_sql(filename)

    statements = sqlglot.parse(
        sql,
        read="trino",
        error_level="RAISE",
    )

    assert len(statements) == 1
    assert isinstance(statements[0], exp.Create)


@pytest.mark.parametrize("filename", GOLD_FILES)
def test_gold_view_name_matches_file(filename):
    """Each SQL file must create its expected view."""

    sql = read_sql(filename)
    expected = view_name(filename)

    expression = sqlglot.parse_one(
        sql,
        read="trino",
    )

    assert isinstance(expression, exp.Create)
    assert expression.args.get("kind", "").upper() == "VIEW"

    created_name = expression.this.name.lower()

    assert created_name == expected


def test_gold_dependency_order():
    """Referenced Gold views must already exist."""

    file_positions = {
        view_name(filename): index
        for index, filename in enumerate(GOLD_FILES)
    }

    for filename in GOLD_FILES:
        current_view = view_name(filename)
        sql = read_sql(filename)

        actual_dependencies = referenced_gold_views(sql)

        expected_dependencies = set(
            DEPENDENCIES[current_view]
        )

        assert actual_dependencies == expected_dependencies, (
            f"Dependency mismatch in {current_view}. "
            f"Expected {expected_dependencies}, "
            f"found {actual_dependencies}"
        )

        for dependency in actual_dependencies:
            assert dependency in file_positions

            assert (
                file_positions[dependency]
                < file_positions[current_view]
            ), (
                f"{current_view} depends on {dependency}, "
                "but the dependency appears later."
            )


def test_unified_view_uses_silver_source():
    """The unified view must include source financials."""

    sql = read_sql(
        "08_gold_unified_bank_monitoring.sql"
    ).lower()

    assert "fdic_financials_named" in sql


def test_unified_view_contains_five_indicators():
    """Final monitoring must include all five signals."""

    sql = read_sql(
        "08_gold_unified_bank_monitoring.sql"
    ).lower()

    required = [
        "deposit_alert",
        "asset_alert",
        "profitability_alert",
        "credit_alert",
        "capital_alert",
        "investigation_priority",
        "has_data_limitation",
    ]

    for column in required:
        assert column in sql, (
            f"Missing unified monitoring field: {column}"
        )
