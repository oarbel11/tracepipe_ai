import duckdb
import networkx as nx
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

class DataQualityIntegration:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or ':memory:'
        self.conn = duckdb.connect(self.db_path)
        self._init_quality_schema()

    def _init_quality_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_metrics (
                table_name VARCHAR,
                metric_type VARCHAR,
                metric_value DOUBLE,
                threshold DOUBLE,
                status VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS observability_signals (
                table_name VARCHAR,
                signal_type VARCHAR,
                severity VARCHAR,
                message VARCHAR,
                timestamp TIMESTAMP
            )
        """)

    def record_quality_metric(self, table_name: str, metric_type: str,
                            value: float, threshold: float):
        status = 'pass' if value >= threshold else 'fail'
        self.conn.execute("""
            INSERT INTO quality_metrics VALUES (?, ?, ?, ?, ?, ?)
        """, [table_name, metric_type, value, threshold, status, datetime.now()])

    def record_signal(self, table_name: str, signal_type: str,
                     severity: str, message: str):
        self.conn.execute("""
            INSERT INTO observability_signals VALUES (?, ?, ?, ?, ?)
        """, [table_name, signal_type, severity, message, datetime.now()])

    def get_table_quality_score(self, table_name: str) -> Dict[str, Any]:
        result = self.conn.execute("""
            SELECT metric_type, metric_value, status
            FROM quality_metrics
            WHERE table_name = ?
            ORDER BY timestamp DESC
            LIMIT 10
        """, [table_name]).fetchall()
        
        if not result:
            return {'score': 100.0, 'metrics': [], 'status': 'unknown'}
        
        pass_count = sum(1 for r in result if r[2] == 'pass')
        score = (pass_count / len(result)) * 100
        return {
            'score': score,
            'metrics': [{'type': r[0], 'value': r[1], 'status': r[2]} for r in result],
            'status': 'healthy' if score >= 80 else 'degraded' if score >= 50 else 'critical'
        }

    def get_recent_signals(self, table_name: str, hours: int = 24) -> List[Dict]:
        cutoff = datetime.now() - timedelta(hours=hours)
        result = self.conn.execute("""
            SELECT signal_type, severity, message, timestamp
            FROM observability_signals
            WHERE table_name = ? AND timestamp > ?
            ORDER BY timestamp DESC
        """, [table_name, cutoff]).fetchall()
        return [{'type': r[0], 'severity': r[1], 'message': r[2], 'time': r[3]} for r in result]
