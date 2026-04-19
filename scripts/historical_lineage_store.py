import duckdb
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import os

class HistoricalLineageStore:
    def __init__(self, db_path: str = 'lineage_history.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                workspace VARCHAR,
                snapshot_time TIMESTAMP,
                lineage_data JSON
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS table_lineage_history (
                id VARCHAR PRIMARY KEY,
                table_fqn VARCHAR,
                upstream_tables JSON,
                downstream_tables JSON,
                snapshot_time TIMESTAMP,
                workspace VARCHAR
            )
        """)

    def snapshot_lineage(self, workspace: str, lineage_data: Dict[str, Any]) -> str:
        snapshot_id = f"{workspace}_{datetime.now().isoformat()}"
        self.conn.execute(
            "INSERT INTO lineage_snapshots VALUES (?, ?, ?, ?)",
            (snapshot_id, workspace, datetime.now(), json.dumps(lineage_data))
        )
        return snapshot_id

    def store_table_lineage(self, table_fqn: str, upstream: List[str], downstream: List[str], workspace: str):
        record_id = f"{table_fqn}_{datetime.now().isoformat()}"
        self.conn.execute(
            "INSERT INTO table_lineage_history VALUES (?, ?, ?, ?, ?, ?)",
            (record_id, table_fqn, json.dumps(upstream), json.dumps(downstream), datetime.now(), workspace)
        )

    def query_historical_lineage(self, table_fqn: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        result = self.conn.execute(
            """SELECT table_fqn, upstream_tables, downstream_tables, snapshot_time, workspace
               FROM table_lineage_history
               WHERE table_fqn = ? AND snapshot_time BETWEEN ? AND ?
               ORDER BY snapshot_time DESC""",
            (table_fqn, start_date, end_date)
        ).fetchall()
        return [{'table': r[0], 'upstream': json.loads(r[1]), 'downstream': json.loads(r[2]), 'time': r[3], 'workspace': r[4]} for r in result]

    def get_snapshots(self, workspace: Optional[str] = None) -> List[Dict[str, Any]]:
        if workspace:
            result = self.conn.execute("SELECT * FROM lineage_snapshots WHERE workspace = ?", (workspace,)).fetchall()
        else:
            result = self.conn.execute("SELECT * FROM lineage_snapshots").fetchall()
        return [{'id': r[0], 'workspace': r[1], 'time': r[2], 'data': json.loads(r[3])} for r in result]

    def close(self):
        self.conn.close()
