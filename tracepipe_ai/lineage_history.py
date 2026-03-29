import duckdb
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path


class LineageHistoryStorage:
    def __init__(self, db_path: str = "lineage_history.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                snapshot_time TIMESTAMP,
                table_name VARCHAR,
                upstream_tables VARCHAR,
                downstream_tables VARCHAR,
                metadata VARCHAR
            )
        """)
        conn.close()

    def store_snapshot(
        self,
        table_name: str,
        upstream: List[str],
        downstream: List[str],
        metadata: Dict[str, Any]
    ) -> str:
        snapshot_time = datetime.utcnow()
        snapshot_id = f"{table_name}_{snapshot_time.isoformat()}"
        
        conn = duckdb.connect(self.db_path)
        conn.execute(
            """INSERT INTO lineage_snapshots VALUES (?, ?, ?, ?, ?, ?)""",
            [
                snapshot_id,
                snapshot_time,
                table_name,
                json.dumps(upstream),
                json.dumps(downstream),
                json.dumps(metadata)
            ]
        )
        conn.close()
        return snapshot_id

    def query_lineage(
        self,
        table_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        conn = duckdb.connect(self.db_path)
        
        query = "SELECT * FROM lineage_snapshots WHERE 1=1"
        params = []
        
        if table_name:
            query += " AND table_name = ?"
            params.append(table_name)
        if start_time:
            query += " AND snapshot_time >= ?"
            params.append(start_time)
        if end_time:
            query += " AND snapshot_time <= ?"
            params.append(end_time)
        
        query += " ORDER BY snapshot_time DESC"
        
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        return [{
            "snapshot_id": r[0],
            "snapshot_time": r[1],
            "table_name": r[2],
            "upstream_tables": json.loads(r[3]),
            "downstream_tables": json.loads(r[4]),
            "metadata": json.loads(r[5])
        } for r in result]
