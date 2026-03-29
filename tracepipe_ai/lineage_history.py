import duckdb
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class LineageHistoryStorage:
    def __init__(self, db_path: str = "lineage_history.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()

    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                snapshot_timestamp TIMESTAMP NOT NULL,
                lineage_data JSON NOT NULL,
                metadata JSON
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_asset_timestamp 
            ON lineage_snapshots(asset_id, snapshot_timestamp)
        """)

    def store_snapshot(self, asset_id: str, asset_type: str, 
                      lineage_data: Dict[str, Any],
                      metadata: Optional[Dict[str, Any]] = None) -> str:
        snapshot_id = f"{asset_id}_{datetime.utcnow().isoformat()}"
        timestamp = datetime.utcnow()
        self.conn.execute("""
            INSERT INTO lineage_snapshots 
            VALUES (?, ?, ?, ?, ?, ?)
        """, [snapshot_id, asset_id, asset_type, timestamp,
              json.dumps(lineage_data), json.dumps(metadata or {})])
        return snapshot_id

    def get_snapshots_for_asset(self, asset_id: str,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None
                               ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM lineage_snapshots WHERE asset_id = ?"
        params = [asset_id]
        if start_date:
            query += " AND snapshot_timestamp >= ?"
            params.append(start_date)
        if end_date:
            query += " AND snapshot_timestamp <= ?"
            params.append(end_date)
        query += " ORDER BY snapshot_timestamp DESC"
        result = self.conn.execute(query, params).fetchall()
        return [{
            "snapshot_id": row[0],
            "asset_id": row[1],
            "asset_type": row[2],
            "snapshot_timestamp": row[3],
            "lineage_data": json.loads(row[4]),
            "metadata": json.loads(row[5])
        } for row in result]

    def time_travel(self, asset_id: str, 
                   target_timestamp: datetime) -> Optional[Dict[str, Any]]:
        result = self.conn.execute("""
            SELECT * FROM lineage_snapshots 
            WHERE asset_id = ? AND snapshot_timestamp <= ?
            ORDER BY snapshot_timestamp DESC LIMIT 1
        """, [asset_id, target_timestamp]).fetchone()
        if not result:
            return None
        return {
            "snapshot_id": result[0],
            "asset_id": result[1],
            "asset_type": result[2],
            "snapshot_timestamp": result[3],
            "lineage_data": json.loads(result[4]),
            "metadata": json.loads(result[5])
        }

    def close(self):
        self.conn.close()
