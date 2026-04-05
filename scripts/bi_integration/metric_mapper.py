from typing import Dict, List, Optional
import duckdb
from config.db_config import get_databricks_connection

class MetricToTableMapper:
    def __init__(self, catalog: str = 'companies_data'):
        self.catalog = catalog
        self.lineage_cache = {}

    def build_lineage_graph(self) -> Dict:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        query = f"""
        SELECT table_catalog, table_schema, table_name, column_name
        FROM {self.catalog}.information_schema.columns
        WHERE table_schema NOT IN ('information_schema')
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        lineage = {}
        for row in rows:
            catalog, schema, table, column = row
            fqn = f"{catalog}.{schema}.{table}"
            if fqn not in lineage:
                lineage[fqn] = []
            lineage[fqn].append(column)
        cursor.close()
        conn.close()
        self.lineage_cache = lineage
        return lineage

    def trace_metric(self, metric_name: str, upstream_tables: List[str],
                     upstream_columns: List[str]) -> Dict:
        if not self.lineage_cache:
            self.build_lineage_graph()
        
        trace = {'metric': metric_name, 'sources': []}
        for table in upstream_tables:
            if table in self.lineage_cache:
                relevant_cols = [c for c in upstream_columns 
                               if c in self.lineage_cache[table]]
                trace['sources'].append({
                    'table': table,
                    'columns': relevant_cols,
                    'available_columns': self.lineage_cache[table]
                })
        return trace

    def validate_metric_source(self, table: str, column: str) -> bool:
        if not self.lineage_cache:
            self.build_lineage_graph()
        return table in self.lineage_cache and column in self.lineage_cache[table]
