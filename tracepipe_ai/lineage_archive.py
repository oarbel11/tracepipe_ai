from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import duckdb
import json
import os


class LineageArchive:
    def __init__(self, db_path: str = "lineage_archive.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()

    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                id VARCHAR PRIMARY KEY,
                event_type VARCHAR,
                source_table VARCHAR,
                target_table VARCHAR,
                timestamp TIMESTAMP,
                metadata VARCHAR,
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

    def archive_lineage(self, events: List[Dict[str, Any]]) -> int:
        archived = 0
        for event in events:
            self.conn.execute("""
                INSERT OR REPLACE INTO lineage_events
                (id, event_type, source_table, target_table, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                event.get("id"),
                event.get("event_type"),
                event.get("source_table"),
                event.get("target_table"),
                event.get("timestamp"),
                json.dumps(event.get("metadata", {}))
            ])
            archived += 1
        return archived

    def query_historical_lineage(
        self,
        start_date: datetime,
        end_date: datetime,
        table_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM lineage_events WHERE timestamp BETWEEN ? AND ?"
        params = [start_date, end_date]
        if table_name:
            query += " AND (source_table = ? OR target_table = ?)"
            params.extend([table_name, table_name])
        result = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]

    def get_statistics(self) -> Dict[str, Any]:
        stats = self.conn.execute("""
            SELECT COUNT(*) as total, MIN(timestamp) as oldest,
            MAX(timestamp) as newest FROM lineage_events
        """).fetchone()
        return {"total": stats[0], "oldest": stats[1], "newest": stats[2]}

    def close(self):
        self.conn.close()
