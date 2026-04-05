import re
from typing import Dict, List, Any, Set

class MetricToTableMapper:
    """Maps BI metrics to Unity Catalog tables using lineage"""
    
    def __init__(self, lineage_data: Dict[str, Any]):
        self.lineage_data = lineage_data
        self.metric_mappings = {}
    
    def map_metric_to_tables(self, metric: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Map a metric to its source tables"""
        query = metric.get('query', '')
        tables = self._extract_tables_from_query(query)
        
        result = []
        for table in tables:
            lineage = self._get_table_lineage(table)
            result.append({
                'metric_name': metric.get('name'),
                'table': table,
                'lineage': lineage
            })
        
        return result
    
    def _extract_tables_from_query(self, query: str) -> Set[str]:
        """Extract table names from SQL query"""
        tables = set()
        from_pattern = r'FROM\s+([a-zA-Z0-9_.]+)'
        join_pattern = r'JOIN\s+([a-zA-Z0-9_.]+)'
        
        tables.update(re.findall(from_pattern, query, re.IGNORECASE))
        tables.update(re.findall(join_pattern, query, re.IGNORECASE))
        
        return tables
    
    def _get_table_lineage(self, table_name: str) -> Dict[str, Any]:
        """Get lineage for a table"""
        if table_name in self.lineage_data:
            return self.lineage_data[table_name]
        
        return {
            'upstream': [],
            'downstream': [],
            'columns': []
        }
    
    def trace_metric_to_source(self, metric_name: str) -> List[str]:
        """Trace a metric back to source tables"""
        if metric_name in self.metric_mappings:
            return self.metric_mappings[metric_name]
        return []
