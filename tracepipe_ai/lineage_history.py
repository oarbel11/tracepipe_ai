import duckdb
import json
from datetime import datetime
from typing import Dict, List, Optional, Any


class LineageHistoryStorage:
    """Persistent storage for historical lineage beyond UC's 1-year window."""

    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Initialize DuckDB schema for lineage history."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                id INTEGER PRIMARY KEY,
                asset_id VARCHAR,
                asset_type VARCHAR,
                snapshot_time TIMESTAMP,
                lineage_data JSON,
                metadata JSON
            )
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS lineage_id_seq START 1
        """)

    def store_lineage(self, asset_id: str, asset_type: str,
                     lineage_data: Dict[str, Any],
                     metadata: Optional[Dict[str, Any]] = None):
        """Store a lineage snapshot."""
        snapshot_time = datetime.utcnow()
        metadata = metadata or {}
        self.conn.execute("""
            INSERT INTO lineage_snapshots
            (id, asset_id, asset_type, snapshot_time, lineage_data, metadata)
            VALUES (nextval('lineage_id_seq'), ?, ?, ?, ?, ?)
        """, [asset_id, asset_type, snapshot_time,
               json.dumps(lineage_data), json.dumps(metadata)])

    def get_lineage_at_time(self, asset_id: str,
                           timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Retrieve lineage state at specific point in time."""
        result = self.conn.execute("""
            SELECT lineage_data FROM lineage_snapshots
            WHERE asset_id = ? AND snapshot_time <= ?
            ORDER BY snapshot_time DESC LIMIT 1
        """, [asset_id, timestamp]).fetchone()
        return json.loads(result[0]) if result else None

    def get_lineage_history(self, asset_id: str,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None) -> List[Dict]:
        """Get all lineage snapshots for an asset within time range."""
        query = "SELECT * FROM lineage_snapshots WHERE asset_id = ?"
        params = [asset_id]
        if start_time:
            query += " AND snapshot_time >= ?"
            params.append(start_time)
        if end_time:
            query += " AND snapshot_time <= ?"
            params.append(end_time)
        query += " ORDER BY snapshot_time"
        results = self.conn.execute(query, params).fetchall()
        return [{
            "id": r[0], "asset_id": r[1], "asset_type": r[2],
            "snapshot_time": r[3], "lineage_data": json.loads(r[4]),
            "metadata": json.loads(r[5])
        } for r in results]

    def close(self):
        """Close the database connection."""
        self.conn.close()
