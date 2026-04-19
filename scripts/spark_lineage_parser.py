import re
from typing import Dict, List, Set, Optional
import networkx as nx
from scripts.column_lineage import ColumnLineageAnalyzer, ColumnNode

class SparkLineageParser:
    def __init__(self):
        self.lineage_graph = nx.DiGraph()
        self.column_analyzer = ColumnLineageAnalyzer()
        self.tables = set()
        
    def parse_sql(self, sql: str) -> Dict:
        tables = self._extract_tables(sql)
        self.tables.update(tables)
        return {'tables': list(tables), 'type': 'sql'}
    
    def parse_notebook(self, filepath: str) -> Dict:
        self.column_analyzer.load_notebook(filepath)
        column_lineage = self.column_analyzer.analyze()
        return {
            'type': 'notebook',
            'udfs': list(column_lineage.udf_definitions.keys()),
            'operations': len(column_lineage.dataframe_ops),
            'column_lineage': column_lineage
        }
    
    def _extract_tables(self, sql: str) -> Set[str]:
        tables = set()
        patterns = [
            r'FROM\s+([\w.]+)',
            r'JOIN\s+([\w.]+)',
            r'INTO\s+(?:TABLE\s+)?([\w.]+)',
            r'UPDATE\s+([\w.]+)',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            tables.update(matches)
        return tables
    
    def get_column_lineage(self, table: str, column: str) -> List[ColumnNode]:
        return self.column_analyzer.get_column_lineage(table, column)
    
    def build_lineage_graph(self) -> nx.DiGraph:
        for op in self.column_analyzer.lineage.dataframe_ops:
            if op['op'] == 'withColumn':
                target = op['column']
                for dep in op.get('dependencies', []):
                    self.lineage_graph.add_edge(dep, target, 
                                              transform=op['op'])
        return self.lineage_graph
    
    def get_upstream_columns(self, column: str) -> List[str]:
        if column not in self.lineage_graph:
            return []
        return list(self.lineage_graph.predecessors(column))
    
    def get_downstream_columns(self, column: str) -> List[str]:
        if column not in self.lineage_graph:
            return []
        return list(self.lineage_graph.successors(column))
    
    def export_lineage(self) -> Dict:
        return {
            'tables': list(self.tables),
            'column_graph': nx.node_link_data(self.lineage_graph),
            'udfs': list(self.column_analyzer.lineage.udf_definitions.keys()),
            'operations': self.column_analyzer.lineage.dataframe_ops
        }
