import pytest
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.unlink(db_path)


def test_store_and_retrieve_lineage(temp_db):
    """Test storing and retrieving lineage history."""
    storage = LineageHistoryStorage(temp_db)
    storage.store_lineage(
        asset_name="sales_table",
        asset_type="table",
        upstream=["raw_sales", "customer_dim"],
        downstream=["revenue_report"],
        metadata={"owner": "data_team", "location": "s3://bucket/path"}
    )

    history = storage.get_lineage_history("sales_table")
    assert len(history) == 1
    assert history[0]["asset_name"] == "sales_table"
    assert history[0]["asset_type"] == "table"


def test_time_travel(temp_db):
    """Test time travel to specific point in past."""
    storage = LineageHistoryStorage(temp_db)
    now = datetime.now()
    past = now - timedelta(days=365)

    storage.store_lineage(
        asset_name="orders_table",
        asset_type="table",
        upstream=["raw_orders"],
        downstream=["analytics_view"]
    )

    result = storage.time_travel("orders_table", now + timedelta(days=1))
    assert result is not None
    assert result["asset_name"] == "orders_table"

    no_result = storage.time_travel("orders_table", past)
    assert no_result is None


def test_multiple_snapshots(temp_db):
    """Test storing multiple snapshots over time."""
    storage = LineageHistoryStorage(temp_db)

    for i in range(3):
        storage.store_lineage(
            asset_name="evolving_table",
            asset_type="table",
            upstream=[f"source_{i}"],
            downstream=[f"target_{i}"]
        )

    history = storage.get_lineage_history("evolving_table")
    assert len(history) == 3


def test_date_range_filter(temp_db):
    """Test filtering lineage by date range."""
    storage = LineageHistoryStorage(temp_db)
    now = datetime.now()

    storage.store_lineage(
        asset_name="filtered_table",
        asset_type="table",
        upstream=["source"],
        downstream=["target"]
    )

    history = storage.get_lineage_history(
        "filtered_table",
        start_date=now - timedelta(hours=1),
        end_date=now + timedelta(hours=1)
    )
    assert len(history) >= 1
