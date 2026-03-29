import networkx as nx
from typing import Dict, List, Set, Tuple
from collections import defaultdict


class ColumnImpactAnalyzer:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.column_metadata = {}
        self.downstream_cache = {}

    def build_graph(self, lineage_data: List[Dict]):
        for data in lineage_data:
            table = data['table']
            for col in data['columns']:
                node_id = f"{table}.{col}"
                self.graph.add_node(node_id, 
                                   table=table, 
                                   column=col,
                                   transformation=data['transformations'].get(col, {}))
                
                for dep in data['dependencies'].get(col, []):
                    self.graph.add_edge(dep, node_id)
                    
                self.column_metadata[node_id] = {
                    'dependencies': data['dependencies'].get(col, []),
                    'transformation': data['transformations'].get(col, {})
                }

    def analyze_impact(self, column_fqn: str) -> Dict:
        if column_fqn not in self.graph:
            return {'error': f'Column {column_fqn} not found'}
        
        downstream = self._get_downstream_columns(column_fqn)
        affected_tables = self._get_affected_tables(downstream)
        criticality = self._assess_criticality(column_fqn, downstream)
        transformation_chain = self._get_transformation_chain(column_fqn)
        
        return {
            'column': column_fqn,
            'downstream_columns': list(downstream),
            'affected_tables': list(affected_tables),
            'impact_count': len(downstream),
            'criticality': criticality,
            'transformation_chain': transformation_chain,
            'risk_score': self._calculate_risk_score(downstream, criticality)
        }

    def _get_downstream_columns(self, column: str) -> Set[str]:
        if column in self.downstream_cache:
            return self.downstream_cache[column]
        downstream = set(nx.descendants(self.graph, column))
        self.downstream_cache[column] = downstream
        return downstream

    def _get_affected_tables(self, columns: Set[str]) -> Set[str]:
        return {col.split('.')[0] for col in columns}

    def _assess_criticality(self, column: str, downstream: Set[str]) -> str:
        count = len(downstream)
        if count > 50:
            return 'critical'
        elif count > 20:
            return 'high'
        elif count > 5:
            return 'medium'
        return 'low'

    def _get_transformation_chain(self, column: str) -> List[Dict]:
        chain = []
        for node in nx.dfs_preorder_nodes(self.graph, column):
            if node in self.column_metadata:
                chain.append({
                    'column': node,
                    'transformation': self.column_metadata[node]['transformation']
                })
        return chain[:10]

    def _calculate_risk_score(self, downstream: Set[str], criticality: str) -> float:
        base_score = len(downstream) * 0.5
        criticality_multiplier = {'low': 1.0, 'medium': 1.5, 'high': 2.0, 'critical': 3.0}
        return min(100.0, base_score * criticality_multiplier.get(criticality, 1.0))

    def get_impact_summary(self) -> Dict:
        summary = defaultdict(int)
        for node in self.graph.nodes():
            downstream = self._get_downstream_columns(node)
            criticality = self._assess_criticality(node, downstream)
            summary[criticality] += 1
        return dict(summary)
