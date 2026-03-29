import duckdb
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path


class LineageHistoryStorage:
    """Persistent storage for historical lineage beyond Unity Catalog's 1-year limit."""

    def __init__(self, db_path: str = "lineage_history.duckdb"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize DuckDB database with lineage schema."""
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                id INTEGER PRIMARY KEY,
                snapshot_date TIMESTAMP,
                source_table VARCHAR,
                target_table VARCHAR,
                lineage_type VARCHAR,
                metadata JSON
            )
        """)
        conn.close()

    def store_snapshot(self, lineage_data: List[Dict[str, Any]]) -> None:
        """Store a lineage snapshot with timestamp."""
        conn = duckdb.connect(self.db_path)
        snapshot_date = datetime.now()
        for record in lineage_data:
            conn.execute("""
                INSERT INTO lineage_snapshots 
                (snapshot_date, source_table, target_table, lineage_type, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, [
                snapshot_date,
                record.get("source_table"),
                record.get("target_table"),
                record.get("lineage_type", "table"),
                json.dumps(record.get("metadata", {}))
            ])
        conn.close()

    def query_historical_lineage(
        self, table_name: str, start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query historical lineage for a table within date range."""
        conn = duckdb.connect(self.db_path)
        query = """
            SELECT * FROM lineage_snapshots
            WHERE source_table = ? OR target_table = ?
        """
        params = [table_name, table_name]
        if start_date:
            query += " AND snapshot_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND snapshot_date <= ?"
            params.append(end_date)
        query += " ORDER BY snapshot_date DESC"
        result = conn.execute(query, params).fetchall()
        conn.close()
        return [{
            "id": r[0], "snapshot_date": str(r[1]), "source_table": r[2],
            "target_table": r[3], "lineage_type": r[4],
            "metadata": json.loads(r[5]) if r[5] else {}
        } for r in result]

    def get_snapshot_dates(self) -> List[str]:
        """Get all available snapshot dates."""
        conn = duckdb.connect(self.db_path)
        result = conn.execute(
            "SELECT DISTINCT snapshot_date FROM lineage_snapshots ORDER BY snapshot_date DESC"
        ).fetchall()
        conn.close()
        return [str(r[0]) for r in result]
