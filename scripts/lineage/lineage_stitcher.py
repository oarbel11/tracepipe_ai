from typing import Dict, List, Optional
from .lineage_graph import LineageGraphBuilder
from .connector_registry import ConnectorRegistry

class LineageStitcher:
    def __init__(self, graph_builder: LineageGraphBuilder, 
                 connector_registry: ConnectorRegistry):
        self.graph = graph_builder
        self.registry = connector_registry

    def stitch_lineage(self, source_configs: List[Dict]) -> Dict:
        for config in source_configs:
            connector_name = config.get('connector')
            connector_config = config.get('config', {})
            
            lineage_data = self.registry.extract_lineage(
                connector_name, connector_config
            )
            self._integrate_lineage(lineage_data, connector_name)
        
        return self.graph.get_lineage_summary()

    def _integrate_lineage(self, lineage_data: Dict, platform: str) -> None:
        for node in lineage_data.get('nodes', []):
            self.graph.add_node(
                node['id'], 
                node['type'], 
                platform, 
                node.get('metadata')
            )
        
        for edge in lineage_data.get('edges', []):
            self.graph.add_edge(
                edge['source'], 
                edge['target'], 
                edge.get('type', 'data_flow')
            )

    def find_cross_platform_paths(self, source_id: str, 
                                  target_id: str) -> List[List[str]]:
        return self.graph.get_all_paths(source_id, target_id)

    def get_platform_summary(self) -> Dict:
        summary = {}
        for node_id, node in self.graph.nodes.items():
            platform = node.platform
            if platform not in summary:
                summary[platform] = {'nodes': 0, 'types': set()}
            summary[platform]['nodes'] += 1
            summary[platform]['types'].add(node.node_type)
        
        for platform in summary:
            summary[platform]['types'] = list(summary[platform]['types'])
        
        return summary
