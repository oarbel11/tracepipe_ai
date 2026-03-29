"""Tests for Historical Lineage & Time Travel feature."""

import pytest
from datetime import datetime, timedelta
import json

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def storage():
    """Create in-memory storage for testing."""
    store = LineageHistoryStorage(":memory:")
    yield store
    store.close()


def test_store_and_retrieve_lineage(storage):
    """Test storing and retrieving lineage snapshots."""
    asset_id = "catalog.schema.table1"
    lineage_data = {
        "upstream": ["catalog.schema.source1"],
        "downstream": ["catalog.schema.target1"]
    }
    metadata = {"user": "test_user", "operation": "CREATE"}

    storage.store_lineage(asset_id, "table", lineage_data, metadata)

    history = storage.get_lineage_history(asset_id)
    assert len(history) == 1
    assert history[0]["asset_id"] == asset_id
    assert history[0]["lineage_data"] == lineage_data
    assert history[0]["metadata"] == metadata


def test_time_travel_query(storage):
    """Test retrieving lineage at specific point in time."""
    asset_id = "catalog.schema.table2"
    now = datetime.utcnow()

    lineage_v1 = {"upstream": ["source1"], "downstream": []}
    storage.store_lineage(asset_id, "table", lineage_v1)

    future = now + timedelta(seconds=2)
    lineage_v2 = {"upstream": ["source1", "source2"], "downstream": []}
    storage.store_lineage(asset_id, "table", lineage_v2)

    past_lineage = storage.get_lineage_at_time(asset_id, now + timedelta(seconds=1))
    assert past_lineage == lineage_v1

    current_lineage = storage.get_lineage_at_time(asset_id, future + timedelta(seconds=1))
    assert current_lineage == lineage_v2


def test_lineage_history_time_range(storage):
    """Test filtering lineage history by time range."""
    asset_id = "catalog.schema.table3"
    base_time = datetime.utcnow()

    for i in range(5):
        lineage = {"upstream": [f"source{i}"], "downstream": []}
        storage.store_lineage(asset_id, "table", lineage)

    all_history = storage.get_lineage_history(asset_id)
    assert len(all_history) == 5

    start_time = base_time + timedelta(seconds=2)
    filtered = storage.get_lineage_history(asset_id, start_time=start_time)
    assert len(filtered) >= 1


def test_nonexistent_asset(storage):
    """Test querying nonexistent asset returns None."""
    result = storage.get_lineage_at_time("nonexistent", datetime.utcnow())
    assert result is None

    history = storage.get_lineage_history("nonexistent")
    assert history == []
