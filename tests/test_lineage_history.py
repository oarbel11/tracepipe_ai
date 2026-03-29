import pytest
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import json
import time

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_lineage.duckdb")
        storage = LineageHistoryStorage(db_path)
        yield storage
        storage.close()


def test_store_and_retrieve_snapshot(temp_db):
    lineage_data = {
        "upstream": ["table_a", "table_b"],
        "downstream": ["table_c"]
    }
    metadata = {"user": "test_user", "operation": "create"}
    
    snapshot_id = temp_db.store_snapshot(
        "asset_123", "table", lineage_data, metadata
    )
    assert snapshot_id is not None
    assert snapshot_id.startswith("asset_123_")
    
    snapshots = temp_db.get_snapshots_for_asset("asset_123")
    assert len(snapshots) == 1
    assert snapshots[0]["asset_id"] == "asset_123"
    assert snapshots[0]["lineage_data"] == lineage_data
    assert snapshots[0]["metadata"] == metadata


def test_multiple_snapshots_ordering(temp_db):
    for i in range(3):
        temp_db.store_snapshot(
            "asset_456", "table", {"version": i}
        )
        time.sleep(0.01)
    
    snapshots = temp_db.get_snapshots_for_asset("asset_456")
    assert len(snapshots) == 3
    assert snapshots[0]["lineage_data"]["version"] == 2
    assert snapshots[2]["lineage_data"]["version"] == 0


def test_time_travel(temp_db):
    base_time = datetime.utcnow()
    
    snapshot_id_1 = temp_db.store_snapshot(
        "asset_789", "table", {"state": "v1"}
    )
    time.sleep(0.1)
    mid_time = datetime.utcnow()
    time.sleep(0.1)
    snapshot_id_2 = temp_db.store_snapshot(
        "asset_789", "table", {"state": "v2"}
    )
    
    result = temp_db.time_travel("asset_789", mid_time)
    assert result is not None
    assert result["lineage_data"]["state"] == "v1"
    
    result_latest = temp_db.time_travel("asset_789", datetime.utcnow())
    assert result_latest["lineage_data"]["state"] == "v2"


def test_date_range_query(temp_db):
    now = datetime.utcnow()
    temp_db.store_snapshot("asset_range", "table", {"day": 1})
    
    future = now + timedelta(days=1)
    snapshots = temp_db.get_snapshots_for_asset(
        "asset_range", end_date=future
    )
    assert len(snapshots) == 1
