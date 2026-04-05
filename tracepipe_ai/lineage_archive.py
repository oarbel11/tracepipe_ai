from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import duckdb
import json
import os


class LineageArchive:
    def __init__(self, archive_db_path: str = "lineage_archive.duckdb"):
        self.db_path = archive_db_path
        self.conn = duckdb.connect(self.db_path)
        self._initialize_schema()

    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                event_id VARCHAR PRIMARY KEY,
                event_type VARCHAR,
                source_table VARCHAR,
                target_table VARCHAR,
                transformation VARCHAR,
                timestamp TIMESTAMP,
                metadata JSON,
                archived_at TIMESTAMP
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

    def archive_lineage(self, lineage_data: Dict[str, Any]) -> str:
        event_id = lineage_data.get("event_id", f"evt_{datetime.utcnow().timestamp()}")
        self.conn.execute("""
            INSERT INTO lineage_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            lineage_data.get("event_type"),
            lineage_data.get("source_table"),
            lineage_data.get("target_table"),
            lineage_data.get("transformation"),
            lineage_data.get("timestamp"),
            json.dumps(lineage_data.get("metadata", {})),
            datetime.utcnow()
        ))
        return event_id

    def query_historical_lineage(
        self, start_date: datetime, end_date: datetime,
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

    def get_compliance_report(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        result = self.conn.execute("""
            SELECT COUNT(*) as total_events,
                   COUNT(DISTINCT source_table) as unique_sources,
                   COUNT(DISTINCT target_table) as unique_targets
            FROM lineage_events WHERE timestamp BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()
        return {
            "total_events": result[0],
            "unique_sources": result[1],
            "unique_targets": result[2],
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat()
        }

    def close(self):
        self.conn.close()
