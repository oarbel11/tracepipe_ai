import duckdb
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path


class LineageHistoryStorage:
    """Persistent storage for historical lineage metadata beyond UC's 1-year window."""

    def __init__(self, db_path: str = "lineage_history.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize DuckDB database with lineage history schema."""
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_history (
                id INTEGER PRIMARY KEY,
                asset_name VARCHAR,
                asset_type VARCHAR,
                upstream_assets VARCHAR,
                downstream_assets VARCHAR,
                metadata VARCHAR,
                captured_at TIMESTAMP,
                source_system VARCHAR
            )
        """)
        conn.close()

    def store_lineage(self, asset_name: str, asset_type: str,
                     upstream: List[str], downstream: List[str],
                     metadata: Optional[Dict[str, Any]] = None,
                     source_system: str = "unity_catalog"):
        """Store lineage snapshot."""
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            INSERT INTO lineage_history
            (asset_name, asset_type, upstream_assets, downstream_assets,
             metadata, captured_at, source_system)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            asset_name, asset_type,
            json.dumps(upstream), json.dumps(downstream),
            json.dumps(metadata or {}),
            datetime.now(), source_system
        ])
        conn.close()

    def get_lineage_history(self, asset_name: str,
                           start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None) -> List[Dict]:
        """Retrieve historical lineage for an asset."""
        conn = duckdb.connect(self.db_path)
        query = "SELECT * FROM lineage_history WHERE asset_name = ?"
        params = [asset_name]

        if start_date:
            query += " AND captured_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND captured_at <= ?"
            params.append(end_date)

        query += " ORDER BY captured_at DESC"
        result = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        conn.close()

        return [dict(zip(columns, row)) for row in result]

    def time_travel(self, asset_name: str, as_of: datetime) -> Optional[Dict]:
        """Get lineage state at a specific point in time."""
        conn = duckdb.connect(self.db_path)
        result = conn.execute("""
            SELECT * FROM lineage_history
            WHERE asset_name = ? AND captured_at <= ?
            ORDER BY captured_at DESC LIMIT 1
        """, [asset_name, as_of]).fetchone()
        conn.close()

        if result:
            columns = ['id', 'asset_name', 'asset_type', 'upstream_assets',
                      'downstream_assets', 'metadata', 'captured_at', 'source_system']
            return dict(zip(columns, result))
        return None
