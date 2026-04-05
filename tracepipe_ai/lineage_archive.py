import json
from datetime import datetime, timedelta
import duckdb
from pathlib import Path
from typing import List, Dict, Any, Optional


class LineageArchive:
    def __init__(self, db_path: str = "lineage_archive.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                id VARCHAR PRIMARY KEY,
                event_time TIMESTAMP,
                table_name VARCHAR,
                upstream_tables VARCHAR,
                downstream_tables VARCHAR,
                operation_type VARCHAR,
                user_name VARCHAR,
                metadata VARCHAR,
                archived_at TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_time 
            ON lineage_events(event_time)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_table_name 
            ON lineage_events(table_name)
        """)

    def archive_lineage(self, lineage_data: Dict[str, Any]) -> str:
        event_id = lineage_data.get("id", f"evt_{datetime.now().isoformat()}")
        self.conn.execute("""
            INSERT INTO lineage_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            lineage_data.get("event_time", datetime.now()),
            lineage_data.get("table_name", ""),
            json.dumps(lineage_data.get("upstream_tables", [])),
            json.dumps(lineage_data.get("downstream_tables", [])),
            lineage_data.get("operation_type", ""),
            lineage_data.get("user_name", ""),
            json.dumps(lineage_data.get("metadata", {})),
            datetime.now()
        ))
        return event_id

    def query_lineage(self, start_date: datetime, end_date: datetime,
                      table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM lineage_events WHERE event_time >= ? AND event_time <= ?"
        params = [start_date, end_date]
        if table_name:
            query += " AND table_name = ?"
            params.append(table_name)
        query += " ORDER BY event_time DESC"
        result = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]

    def get_table_history(self, table_name: str) -> List[Dict[str, Any]]:
        result = self.conn.execute("""
            SELECT * FROM lineage_events 
            WHERE table_name = ? 
            ORDER BY event_time DESC
        """, [table_name]).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]

    def close(self):
        self.conn.close()
