import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json


class LineageHistoryStore:
    def __init__(self, db_path: str = "lineage_history.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                captured_at TIMESTAMP,
                source_table VARCHAR,
                target_table VARCHAR,
                source_column VARCHAR,
                target_column VARCHAR,
                lineage_type VARCHAR,
                metadata JSON
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_captured_at 
            ON lineage_snapshots(captured_at)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_source_target 
            ON lineage_snapshots(source_table, target_table)
        """)

    def export_lineage(self, lineage_data: pd.DataFrame) -> int:
        timestamp = datetime.now()
        records = []
        for _, row in lineage_data.iterrows():
            record = {
                'snapshot_id': f"{row.get('source_table', '')}_{row.get('target_table', '')}_{timestamp.isoformat()}",
                'captured_at': timestamp,
                'source_table': row.get('source_table', ''),
                'target_table': row.get('target_table', ''),
                'source_column': row.get('source_column', ''),
                'target_column': row.get('target_column', ''),
                'lineage_type': row.get('lineage_type', 'table'),
                'metadata': json.dumps(row.to_dict())
            }
            records.append(record)
        
        if records:
            df = pd.DataFrame(records)
            self.conn.execute("INSERT INTO lineage_snapshots SELECT * FROM df")
        return len(records)

    def time_travel(self, target_date: datetime, 
                    table_name: Optional[str] = None) -> pd.DataFrame:
        query = "SELECT * FROM lineage_snapshots WHERE captured_at <= ?"
        params = [target_date]
        
        if table_name:
            query += " AND (source_table = ? OR target_table = ?)"
            params.extend([table_name, table_name])
        
        query += " ORDER BY captured_at DESC"
        return self.conn.execute(query, params).df()

    def get_lineage_evolution(self, table_name: str, 
                              start_date: datetime,
                              end_date: datetime) -> List[Dict]:
        result = self.conn.execute("""
            SELECT captured_at, source_table, target_table, 
                   source_column, target_column, lineage_type
            FROM lineage_snapshots
            WHERE (source_table = ? OR target_table = ?)
              AND captured_at BETWEEN ? AND ?
            ORDER BY captured_at ASC
        """, [table_name, table_name, start_date, end_date]).fetchall()
        
        return [{
            'timestamp': row[0],
            'source': row[1],
            'target': row[2],
            'source_col': row[3],
            'target_col': row[4],
            'type': row[5]
        } for row in result]

    def close(self):
        self.conn.close()
