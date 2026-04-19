import json
import sqlite3
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path

class HistoricalLineageStore:
    def __init__(self, db_path: str = 'lineage_history.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database for historical lineage."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                notebook_path TEXT NOT NULL,
                lineage_data TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_notebook_timestamp 
            ON lineage_snapshots(notebook_path, timestamp)
        ''')
        conn.commit()
        conn.close()

    def snapshot_lineage(self, notebook_path: str, lineage_data: Dict[str, Any]):
        """Store a lineage snapshot."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        timestamp = datetime.utcnow().isoformat()
        cursor.execute(
            'INSERT INTO lineage_snapshots (timestamp, notebook_path, lineage_data) VALUES (?, ?, ?)',
            (timestamp, notebook_path, json.dumps(lineage_data))
        )
        conn.commit()
        conn.close()

    def query_historical_lineage(self, notebook_path: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Query historical lineage for a notebook."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = 'SELECT timestamp, lineage_data FROM lineage_snapshots WHERE notebook_path = ?'
        params = [notebook_path]
        if start_date:
            query += ' AND timestamp >= ?'
            params.append(start_date)
        if end_date:
            query += ' AND timestamp <= ?'
            params.append(end_date)
        query += ' ORDER BY timestamp DESC'
        cursor.execute(query, params)
        results = []
        for row in cursor.fetchall():
            results.append({
                'timestamp': row[0],
                'lineage_data': json.loads(row[1])
            })
        conn.close()
        return results

    def cleanup_old_snapshots(self, retention_days: int = 730):
        """Remove snapshots older than retention period."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cutoff = datetime.utcnow().timestamp() - (retention_days * 86400)
        cursor.execute('DELETE FROM lineage_snapshots WHERE timestamp < ?', (cutoff,))
        conn.commit()
        conn.close()
