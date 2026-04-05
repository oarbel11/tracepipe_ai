from datetime import datetime, timedelta
from typing import Dict, List, Optional
import duckdb
import json
import os


class LineageArchive:
    def __init__(self, db_path: str = "lineage_archive.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                event_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                source_table VARCHAR,
                target_table VARCHAR,
                operation_type VARCHAR,
                user_name VARCHAR,
                metadata JSON,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON lineage_events(timestamp)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_source ON lineage_events(source_table)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_target ON lineage_events(target_table)
        """)

    def archive_lineage(
        self, event_id: str, timestamp: datetime, source_table: str,
        target_table: str, operation_type: str, user_name: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        try:
            self.conn.execute(
                """INSERT INTO lineage_events 
                   (event_id, timestamp, source_table, target_table, 
                    operation_type, user_name, metadata)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [event_id, timestamp, source_table, target_table,
                 operation_type, user_name, json.dumps(metadata or {})]
            )
            return True
        except Exception:
            return False

    def query_lineage(
        self, start_date: datetime, end_date: datetime,
        table_name: Optional[str] = None
    ) -> List[Dict]:
        query = "SELECT * FROM lineage_events WHERE timestamp BETWEEN ? AND ?"
        params = [start_date, end_date]
        if table_name:
            query += " AND (source_table = ? OR target_table = ?)"
            params.extend([table_name, table_name])
        result = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]

    def get_statistics(self) -> Dict:
        stats = self.conn.execute(
            "SELECT COUNT(*) as total, MIN(timestamp) as oldest FROM lineage_events"
        ).fetchone()
        return {"total_events": stats[0], "oldest_event": stats[1]}

    def close(self):
        self.conn.close()
