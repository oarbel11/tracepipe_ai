"""Long-term lineage archiving module for Databricks Unity Catalog."""
import json
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


class LineageArchive:
    """Manages long-term archival of Databricks lineage metadata."""

    def __init__(self, db_path: str = "data/lineage_archive.duckdb"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Initialize archive database schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                id INTEGER PRIMARY KEY,
                event_time TIMESTAMP,
                source_table VARCHAR,
                target_table VARCHAR,
                operation VARCHAR,
                metadata JSON,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_time 
            ON lineage_events(event_time)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_source 
            ON lineage_events(source_table)
        """)

    def archive_lineage(self, lineage_data: List[Dict[str, Any]]) -> int:
        """Archive lineage events to long-term storage."""
        count = 0
        for event in lineage_data:
            self.conn.execute("""
                INSERT INTO lineage_events 
                (event_time, source_table, target_table, operation, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, [
                event.get('event_time', datetime.now()),
                event.get('source_table', ''),
                event.get('target_table', ''),
                event.get('operation', 'unknown'),
                json.dumps(event.get('metadata', {}))
            ])
            count += 1
        return count

    def query_historical(self, start_date: str, end_date: str,
                        table_filter: Optional[str] = None) -> List[Dict]:
        """Query historical lineage within date range."""
        query = "SELECT * FROM lineage_events WHERE event_time BETWEEN ? AND ?"
        params = [start_date, end_date]
        if table_filter:
            query += " AND (source_table LIKE ? OR target_table LIKE ?)"
            params.extend([f"%{table_filter}%", f"%{table_filter}%"])
        result = self.conn.execute(query, params).fetchall()
        columns = ['id', 'event_time', 'source_table', 'target_table',
                   'operation', 'metadata', 'archived_at']
        return [dict(zip(columns, row)) for row in result]

    def close(self):
        """Close database connection."""
        self.conn.close()
