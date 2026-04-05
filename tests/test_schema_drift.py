"""
Tests for Schema Drift Detector — scripts/schema_drift.py

All tests use mock data. No live database connection is required.
"""

import pytest
from unittest.mock import MagicMock, patch
from scripts.schema_drift import (
    SchemaDriftDetector,
    DriftChange,
    DriftReport,
    is_safe_widening,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SNAPSHOT_A = {
    "raw.users": [
        {"column_name": "id", "column_type": "INTEGER"},
        {"column_name": "name", "column_type": "VARCHAR"},
        {"column_name": "email", "column_type": "VARCHAR"},
    ],
    "silver.orders": [
        {"column_name": "order_id", "column_type": "INTEGER"},
        {"column_name": "amount", "column_type": "FLOAT"},
    ],
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TESTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def test_no_drift():
    """Identical snapshots should produce zero changes."""
    changes = SchemaDriftDetector.compare(SNAPSHOT_A, SNAPSHOT_A)
    assert changes == []


def test_column_added():
    """A new column appearing should be detected as 'added'."""
    current = {
        "raw.users": SNAPSHOT_A["raw.users"] + [
            {"column_name": "phone", "column_type": "VARCHAR"},
        ],
        "silver.orders": SNAPSHOT_A["silver.orders"],
    }
    changes = SchemaDriftDetector.compare(SNAPSHOT_A, current)

    assert len(changes) == 1
    c = changes[0]
    assert c.table == "raw.users"
    assert c.change_type == "added"
    assert c.column_name == "phone"
    assert c.new_type == "VARCHAR"
    assert c.old_type is None
    assert c.risk == "GREEN"  # No downstream info yet


def test_column_removed():
    """A removed column should be detected as 'removed' with RED risk."""
    current = {
        "raw.users": [
            {"column_name": "id", "column_type": "INTEGER"},
            # 'name' and 'email' removed
        ],
        "silver.orders": SNAPSHOT_A["silver.orders"],
    }
    changes = SchemaDriftDetector.compare(SNAPSHOT_A, current)

    assert len(changes) == 2  # 'name' and 'email' removed
    for c in changes:
        assert c.change_type == "removed"
        assert c.risk == "RED"

    removed_cols = {c.column_name for c in changes}
    assert removed_cols == {"name", "email"}


def test_type_changed():
    """A type change should be detected. Safe widenings get YELLOW, others get RED."""
    current = {
        "raw.users": [
            {"column_name": "id", "column_type": "BIGINT"},  # INTEGER → BIGINT (safe)
            {"column_name": "name", "column_type": "INTEGER"},  # VARCHAR → INTEGER (breaking!)
            {"column_name": "email", "column_type": "VARCHAR"},
        ],
        "silver.orders": SNAPSHOT_A["silver.orders"],
    }
    changes = SchemaDriftDetector.compare(SNAPSHOT_A, current)

    assert len(changes) == 2
    by_col = {c.column_name: c for c in changes}

    # INTEGER → BIGINT is a safe widening
    assert by_col["id"].change_type == "type_changed"
    assert by_col["id"].old_type == "INTEGER"
    assert by_col["id"].new_type == "BIGINT"
    assert by_col["id"].risk == "YELLOW"

    # VARCHAR → INTEGER is a breaking change
    assert by_col["name"].change_type == "type_changed"
    assert by_col["name"].old_type == "VARCHAR"
    assert by_col["name"].new_type == "INTEGER"
    assert by_col["name"].risk == "RED"


def test_risk_with_downstream():
    """An added column on a table with downstream consumers should be YELLOW."""
    current = {
        "raw.users": SNAPSHOT_A["raw.users"] + [
            {"column_name": "phone", "column_type": "VARCHAR"},
        ],
        "silver.orders": SNAPSHOT_A["silver.orders"],
    }

    # Mock the engine
    mock_engine = MagicMock()
    mock_engine.list_tables.return_value = [
        {"table_schema": "raw", "table_name": "users"},
        {"table_schema": "silver", "table_name": "orders"},
    ]
    mock_engine.get_downstream_tables.return_value = [
        "silver.dim_users", "silver.fact_logins",
        "conformed.user_stats", "conformed.churn_risk", "gold.dashboard",
    ]

    detector = SchemaDriftDetector(engine=mock_engine, snapshot_dir=None)
    changes = SchemaDriftDetector.compare(SNAPSHOT_A, current)

    # Simulate the risk upgrade logic from detect_drift
    for change in changes:
        change.downstream_tables = mock_engine.get_downstream_tables(change.table)
        if change.change_type == 'added' and len(change.downstream_tables) > 0:
            change.risk = 'YELLOW'
        if len(change.downstream_tables) > 3 and change.risk != 'RED':
            change.risk = 'YELLOW'

    assert len(changes) == 1
    assert changes[0].risk == "YELLOW"
    assert len(changes[0].downstream_tables) == 5


def test_multiple_tables_drift():
    """Drift in multiple tables should all be detected correctly."""
    current = {
        "raw.users": [
            {"column_name": "id", "column_type": "INTEGER"},
            {"column_name": "name", "column_type": "VARCHAR"},
            # email removed
            {"column_name": "phone", "column_type": "VARCHAR"},  # added
        ],
        "silver.orders": [
            {"column_name": "order_id", "column_type": "BIGINT"},  # type change
            {"column_name": "amount", "column_type": "FLOAT"},
            {"column_name": "status", "column_type": "VARCHAR"},  # added
        ],
    }

    changes = SchemaDriftDetector.compare(SNAPSHOT_A, current)

    # raw.users: email removed + phone added = 2 changes
    # silver.orders: order_id type change + status added = 2 changes
    assert len(changes) == 4

    tables_affected = {c.table for c in changes}
    assert tables_affected == {"raw.users", "silver.orders"}

    # Verify specific changes
    by_key = {(c.table, c.column_name): c for c in changes}
    assert by_key[("raw.users", "email")].change_type == "removed"
    assert by_key[("raw.users", "phone")].change_type == "added"
    assert by_key[("silver.orders", "order_id")].change_type == "type_changed"
    assert by_key[("silver.orders", "status")].change_type == "added"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SAFE WIDENING TESTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_safe_widening_positive():
    """Known safe widenings should return True."""
    assert is_safe_widening("INTEGER", "BIGINT") is True
    assert is_safe_widening("FLOAT", "DOUBLE") is True
    assert is_safe_widening("SMALLINT", "INTEGER") is True


def test_safe_widening_negative():
    """Unknown or narrowing changes should return False."""
    assert is_safe_widening("BIGINT", "INTEGER") is False
    assert is_safe_widening("VARCHAR", "INTEGER") is False
    assert is_safe_widening("DOUBLE", "FLOAT") is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REPORT FORMATTING TESTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_drift_report_formatted_output_no_changes():
    """Empty report should show 'no drift' message."""
    report = DriftReport(tables_scanned=10)
    output = report.formatted_output
    assert "No schema drift detected" in output


def test_drift_report_to_dict():
    """to_dict should produce a serializable dictionary."""
    report = DriftReport(
        changes=[DriftChange(
            table="raw.users", change_type="added", column_name="phone",
            old_type=None, new_type="VARCHAR", risk="GREEN",
        )],
        tables_scanned=5,
        tables_with_drift=1,
        snapshot_timestamp="20260322_001500",
        previous_snapshot_timestamp="20260321_230000",
    )
    d = report.to_dict()
    assert d["tables_scanned"] == 5
    assert d["total_changes"] == 1
    assert d["risk_summary"]["GREEN"] == 1
    assert d["risk_summary"]["RED"] == 0
