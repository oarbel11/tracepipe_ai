import duckdb
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class LineageHistoryStorage:
    def __init__(self, db_path: str = "lineage_history.duckdb"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                snapshot_date TIMESTAMP,
                source_table VARCHAR,
                target_table VARCHAR,
                source_type VARCHAR,
                target_type VARCHAR,
                metadata JSON
            )
        """)
        conn.close()

    def store_lineage(self, lineage_data: List[Dict[str, Any]]) -> str:
        snapshot_id = datetime.now().isoformat()
        conn = duckdb.connect(self.db_path)
        
        for record in lineage_data:
            conn.execute("""
                INSERT INTO lineage_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id,
                datetime.now(),
                record.get('source_table', ''),
                record.get('target_table', ''),
                record.get('source_type', ''),
                record.get('target_type', ''),
                json.dumps(record.get('metadata', {}))
            ))
        
        conn.close()
        return snapshot_id

    def query_lineage(self, start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = duckdb.connect(self.db_path)
        
        query = "SELECT * FROM lineage_snapshots WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND snapshot_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND snapshot_date <= ?"
            params.append(end_date)
        if table_name:
            query += " AND (source_table = ? OR target_table = ?)"
            params.extend([table_name, table_name])
        
        result = conn.execute(query, params).fetchall()
        columns = ['snapshot_id', 'snapshot_date', 'source_table',
                   'target_table', 'source_type', 'target_type', 'metadata']
        
        conn.close()
        return [dict(zip(columns, row)) for row in result]
