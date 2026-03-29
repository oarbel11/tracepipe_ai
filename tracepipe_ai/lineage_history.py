import duckdb
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class LineageHistoryStorage:
    """Persistent storage for lineage metadata beyond UC's 1-year limit."""

    def __init__(self, db_path: str = "lineage_history.duckdb"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize DuckDB database with lineage table."""
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_history (
                id INTEGER PRIMARY KEY,
                source_table VARCHAR,
                target_table VARCHAR,
                timestamp TIMESTAMP,
                metadata JSON,
                captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS lineage_id_seq START 1
        """)
        conn.close()

    def store_lineage(self, source: str, target: str, 
                     metadata: Dict[str, Any]) -> int:
        """Store a lineage record."""
        conn = duckdb.connect(self.db_path)
        result = conn.execute("""
            INSERT INTO lineage_history 
            (id, source_table, target_table, timestamp, metadata)
            VALUES (nextval('lineage_id_seq'), ?, ?, ?, ?)
            RETURNING id
        """, [source, target, datetime.now(), json.dumps(metadata)]).fetchone()
        conn.close()
        return result[0]

    def query_lineage(self, table: Optional[str] = None,
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Query lineage history with filters."""
        conn = duckdb.connect(self.db_path)
        query = "SELECT * FROM lineage_history WHERE 1=1"
        params = []
        
        if table:
            query += " AND (source_table = ? OR target_table = ?)"
            params.extend([table, table])
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date)
        
        query += " ORDER BY timestamp DESC"
        results = conn.execute(query, params).fetchall()
        conn.close()
        
        return [
            {
                "id": r[0],
                "source_table": r[1],
                "target_table": r[2],
                "timestamp": r[3],
                "metadata": json.loads(r[4]) if r[4] else {},
                "captured_at": r[5]
            }
            for r in results
        ]
