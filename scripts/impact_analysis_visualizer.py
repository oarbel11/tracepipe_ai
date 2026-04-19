import networkx as nx
from typing import Dict, List, Any, Optional

class ImpactAnalysisVisualizer:
    def __init__(self, lineage_manager):
        self.lineage_manager = lineage_manager

    def analyze_impact(self, node: str, change_type: str, details: Optional[Dict] = None) -> Dict[str, Any]:
        graph = self.lineage_manager.graph
        if node not in graph:
            return {'node': node, 'change_type': change_type, 'impacted_nodes': [], 'impact_count': 0}
        
        impacted = set()
        if change_type in ['schema_change', 'column_drop', 'column_add', 'type_change']:
            try:
                descendants = nx.descendants(graph, node)
                impacted.update(descendants)
            except:
                pass
        
        return {
            'node': node,
            'change_type': change_type,
            'details': details or {},
            'impacted_nodes': list(impacted),
            'impact_count': len(impacted)
        }

    def analyze_column_impact(self, table: str, column: str, change_type: str) -> Dict[str, Any]:
        entity = f"{table}.{column}"
        return self.analyze_impact(entity, change_type, {'table': table, 'column': column})

    def get_dependency_path(self, source: str, target: str) -> List[List[str]]:
        graph = self.lineage_manager.graph
        if source not in graph or target not in graph:
            return []
        try:
            paths = list(nx.all_simple_paths(graph, source, target, cutoff=10))
            return paths[:5]
        except:
            return []

    def calculate_blast_radius(self, node: str) -> Dict[str, Any]:
        graph = self.lineage_manager.graph
        if node not in graph:
            return {'node': node, 'direct_downstream': 0, 'total_downstream': 0, 'depth': 0}
        
        direct = list(graph.successors(node))
        try:
            total = list(nx.descendants(graph, node))
            depth = 0
            if total:
                for target in total:
                    try:
                        path_len = nx.shortest_path_length(graph, node, target)
                        depth = max(depth, path_len)
                    except:
                        pass
        except:
            total = direct
            depth = 1 if direct else 0
        
        return {
            'node': node,
            'direct_downstream': len(direct),
            'total_downstream': len(total),
            'depth': depth,
            'impacted_nodes': total
        }
