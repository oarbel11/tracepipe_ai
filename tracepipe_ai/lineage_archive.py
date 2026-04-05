import json
from datetime import datetime, timedelta
import duckdb
from typing import List, Dict, Any, Optional
from pathlib import Path


class LineageArchive:
    def __init__(self, db_path: str = "lineage_archive.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                id INTEGER PRIMARY KEY,
                event_id VARCHAR,
                event_type VARCHAR,
                source_table VARCHAR,
                target_table VARCHAR,
                timestamp TIMESTAMP,
                metadata JSON,
                indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON lineage_events(timestamp)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_source 
            ON lineage_events(source_table)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_target 
            ON lineage_events(target_table)
        """)

    def archive_event(self, event: Dict[str, Any]) -> None:
        self.conn.execute("""
            INSERT INTO lineage_events 
            (event_id, event_type, source_table, target_table, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            event.get("event_id"),
            event.get("event_type"),
            event.get("source_table"),
            event.get("target_table"),
            event.get("timestamp"),
            json.dumps(event.get("metadata", {}))
        ])

    def query_by_date_range(self, start: datetime, end: datetime) -> List[Dict]:
        result = self.conn.execute("""
            SELECT * FROM lineage_events 
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """, [start, end]).fetchall()
        return [self._row_to_dict(row) for row in result]

    def query_by_table(self, table_name: str) -> List[Dict]:
        result = self.conn.execute("""
            SELECT * FROM lineage_events 
            WHERE source_table = ? OR target_table = ?
            ORDER BY timestamp DESC
        """, [table_name, table_name]).fetchall()
        return [self._row_to_dict(row) for row in result]

    def _row_to_dict(self, row) -> Dict:
        return {
            "id": row[0], "event_id": row[1], "event_type": row[2],
            "source_table": row[3], "target_table": row[4],
            "timestamp": row[5], "metadata": json.loads(row[6]),
            "indexed_at": row[7]
        }

    def close(self):
        self.conn.close()
