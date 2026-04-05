import os
import yaml
import networkx as nx
from databricks import sql
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
import json


class OperationalLineageTracker:
    def __init__(self, config_path='config/config.yml'):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.db_config = config.get('databricks', {})
        self.graph = nx.DiGraph()
        self.code_assets = {}
        self.data_assets = {}

    def _get_connection(self):
        return sql.connect(
            server_hostname=self.db_config.get('server_hostname'),
            http_path=self.db_config.get('http_path'),
            access_token=self.db_config.get('access_token')
        )

    def capture_lineage(self, catalog: str, days_back: int = 7) -> nx.DiGraph:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = f"""
            SELECT query_text, user_name, statement_type, 
                   executed_as_user_name, start_time
            FROM system.query.history
            WHERE start_time >= '{start_date.isoformat()}'
              AND (statement_type IN ('CREATE_TABLE', 'INSERT', 'MERGE', 'UPDATE')
                   OR query_text LIKE '%INSERT%' OR query_text LIKE '%CREATE%')
            ORDER BY start_time DESC
            LIMIT 1000
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                self._parse_query_lineage(row)
            
            self._capture_table_metadata(cursor, catalog)
        
        return self.graph

    def _parse_query_lineage(self, query_row):
        query_text, user, stmt_type, exec_user, timestamp = query_row
        tables_written = self._extract_tables_written(query_text, stmt_type)
        tables_read = self._extract_tables_read(query_text)
        
        code_id = f"query_{hash(query_text[:100])}_{timestamp}"
        self.code_assets[code_id] = {
            'type': 'query',
            'user': user,
            'statement_type': stmt_type,
            'timestamp': str(timestamp)
        }
        self.graph.add_node(code_id, node_type='code', **self.code_assets[code_id])
        
        for table in tables_written:
            self.graph.add_node(table, node_type='data')
            self.graph.add_edge(code_id, table, relationship='writes')
        
        for table in tables_read:
            self.graph.add_node(table, node_type='data')
            self.graph.add_edge(table, code_id, relationship='reads')

    def _extract_tables_written(self, query: str, stmt_type: str) -> Set[str]:
        import re
        tables = set()
        patterns = [
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)',
            r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([\w.]+)',
            r'MERGE\s+INTO\s+([\w.]+)'
        ]
        for pattern in patterns:
            matches = re.findall(pattern, query.upper())
            tables.update(m.lower() for m in matches)
        return tables

    def _extract_tables_read(self, query: str) -> Set[str]:
        import re
        tables = set()
        pattern = r'FROM\s+([\w.]+)'
        matches = re.findall(pattern, query.upper())
        tables.update(m.lower() for m in matches)
        return tables

    def _capture_table_metadata(self, cursor, catalog: str):
        query = f"""
        SELECT table_catalog, table_schema, table_name, created_by
        FROM {catalog}.information_schema.tables
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            cat, schema, table, created_by = row
            full_name = f"{cat}.{schema}.{table}"
            self.data_assets[full_name] = {
                'created_by': created_by,
                'catalog': cat,
                'schema': schema
            }

    def get_upstream_code(self, table_name: str) -> List[Dict]:
        if table_name not in self.graph:
            return []
        predecessors = list(self.graph.predecessors(table_name))
        return [self.code_assets.get(p, {}) for p in predecessors 
                if self.graph.nodes[p].get('node_type') == 'code']

    def visualize_graph(self, output_path: str):
        html = '<html><body><h2>Operational Lineage</h2>'
        html += '<div style="font-family:monospace">'
        for node in self.graph.nodes():
            if self.graph.nodes[node].get('node_type') == 'code':
                html += f'<p><b>CODE:</b> {node}</p>'
                for succ in self.graph.successors(node):
                    html += f'<p style="margin-left:20px">→ writes: {succ}</p>'
        html += '</div></body></html>'
        with open(output_path, 'w') as f:
            f.write(html)
