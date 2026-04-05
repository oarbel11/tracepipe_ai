import networkx as nx
from typing import Dict, List, Set, Any


class ImpactAnalysisMapper:
    def __init__(self):
        self.lineage_graph = nx.DiGraph()
        self.asset_registry = {}

    def build_lineage_graph(self, catalog_metadata: Dict) -> nx.DiGraph:
        self.lineage_graph.clear()
        
        for table_name, metadata in catalog_metadata.items():
            self.lineage_graph.add_node(table_name, 
                                       type='table', 
                                       metadata=metadata)
            
            upstream = metadata.get('upstream_tables', [])
            for upstream_table in upstream:
                self.lineage_graph.add_edge(upstream_table, table_name)
            
            downstream_assets = metadata.get('downstream_assets', [])
            for asset in downstream_assets:
                asset_id = f"{asset['type']}:{asset['name']}"
                self.lineage_graph.add_node(asset_id, type=asset['type'])
                self.lineage_graph.add_edge(table_name, asset_id)
        
        return self.lineage_graph

    def get_downstream_impact(self, target: str, depth: int = 10) -> Dict:
        if target not in self.lineage_graph:
            self._register_asset(target)
        
        impact = {
            'tables': [],
            'dashboards': [],
            'reports': [],
            'ml_models': [],
            'notebooks': [],
            'jobs': []
        }
        
        try:
            descendants = nx.descendants(self.lineage_graph, target)
            
            for node in descendants:
                node_type = self.lineage_graph.nodes[node].get('type', 'table')
                node_data = {'name': node, 'distance': 
                           nx.shortest_path_length(self.lineage_graph, target, node)}
                
                if node_type == 'table':
                    impact['tables'].append(node_data)
                elif node_type in impact:
                    impact[node_type].append(node_data)
        except nx.NetworkXError:
            pass
        
        return impact

    def get_upstream_dependencies(self, target: str) -> List[str]:
        if target not in self.lineage_graph:
            return []
        return list(nx.ancestors(self.lineage_graph, target))

    def calculate_blast_radius_score(self, target: str) -> float:
        impact = self.get_downstream_impact(target)
        total = sum(len(assets) for assets in impact.values())
        
        weights = {'ml_models': 2.0, 'dashboards': 1.5, 'reports': 1.5,
                  'tables': 1.0, 'notebooks': 1.0, 'jobs': 1.2}
        
        weighted_score = sum(len(impact[k]) * weights.get(k, 1.0) 
                            for k in impact)
        return min(weighted_score / 10.0, 1.0)

    def _register_asset(self, asset_id: str):
        self.lineage_graph.add_node(asset_id, type='table')
        self.asset_registry[asset_id] = {'registered': True}
