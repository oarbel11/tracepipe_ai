import duckdb
from datetime import datetime
from typing import List, Dict, Optional
import yaml


class LineageQueryEngine:
    def __init__(self, config_path: str = "config/config.yml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.db_path = self.config.get('lineage_archive_db', 'lineage_archive.duckdb')
        self.conn = duckdb.connect(self.db_path, read_only=True)

    def query_entity_lineage(self, entity_name: str, 
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> List[Dict]:
        query = """
            SELECT edge_id, source_entity, target_entity, 
                   lineage_type, captured_at, metadata
            FROM lineage_edges
            WHERE source_entity = ? OR target_entity = ?
        """
        params = [entity_name, entity_name]
        
        if start_date:
            query += " AND captured_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND captured_at <= ?"
            params.append(end_date)
        
        query += " ORDER BY captured_at DESC"
        result = self.conn.execute(query, params).fetchall()
        return [{
            'edge_id': r[0], 'source': r[1], 'target': r[2],
            'type': r[3], 'timestamp': r[4], 'metadata': r[5]
        } for r in result]

    def get_lineage_timeline(self, entity_name: str) -> List[Dict]:
        query = """
            SELECT DATE_TRUNC('day', captured_at) as day, 
                   COUNT(*) as edge_count,
                   COUNT(DISTINCT source_entity) as upstream_count,
                   COUNT(DISTINCT target_entity) as downstream_count
            FROM lineage_edges
            WHERE source_entity LIKE ? OR target_entity LIKE ?
            GROUP BY day
            ORDER BY day DESC
        """
        pattern = f"%{entity_name}%"
        result = self.conn.execute(query, [pattern, pattern]).fetchall()
        return [{
            'date': r[0], 'edges': r[1], 'upstream': r[2], 'downstream': r[3]
        } for r in result]

    def query_snapshots(self, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> List[Dict]:
        query = "SELECT snapshot_id, extracted_at, source_system FROM lineage_snapshots WHERE 1=1"
        params = []
        if start_date:
            query += " AND extracted_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND extracted_at <= ?"
            params.append(end_date)
        query += " ORDER BY extracted_at DESC"
        result = self.conn.execute(query, params).fetchall()
        return [{'id': r[0], 'timestamp': r[1], 'source': r[2]} for r in result]

    def audit_report(self, entity_name: str, months_back: int = 12) -> Dict:
        query = """
            SELECT MIN(captured_at) as first_seen, MAX(captured_at) as last_seen,
                   COUNT(DISTINCT snapshot_id) as snapshot_count,
                   COUNT(*) as total_edges
            FROM lineage_edges
            WHERE source_entity = ? OR target_entity = ?
        """
        result = self.conn.execute(query, [entity_name, entity_name]).fetchone()
        return {
            'entity': entity_name, 'first_seen': result[0], 'last_seen': result[1],
            'snapshots': result[2], 'total_edges': result[3]
        }
