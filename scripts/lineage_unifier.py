import networkx as nx
from typing import List, Dict, Any
from connectors import ConnectorRegistry

class LineageUnifier:
    def __init__(self):
        self.unified_graph = nx.DiGraph()
        self.mapping_rules = {}

    def add_lineage_source(self, connector_name: str, config: Dict[str, Any]):
        connector = ConnectorRegistry.get_connector(connector_name, config)
        
        if not connector.validate_config():
            raise ValueError(f"Invalid config for connector '{connector_name}'")
        
        lineage_graph = connector.extract_lineage()
        self.unified_graph = nx.compose(self.unified_graph, lineage_graph)

    def add_mapping_rule(self, source_pattern: str, target_pattern: str, system_a: str, system_b: str):
        rule_id = f"{system_a}:{system_b}"
        if rule_id not in self.mapping_rules:
            self.mapping_rules[rule_id] = []
        self.mapping_rules[rule_id].append({
            'source_pattern': source_pattern,
            'target_pattern': target_pattern
        })

    def apply_cross_system_mappings(self):
        for rule_id, rules in self.mapping_rules.items():
            for rule in rules:
                self._apply_mapping(rule['source_pattern'], rule['target_pattern'])

    def _apply_mapping(self, source_pattern: str, target_pattern: str):
        matched_nodes = [n for n in self.unified_graph.nodes() if source_pattern in n]
        target_nodes = [n for n in self.unified_graph.nodes() if target_pattern in n]
        
        for src in matched_nodes:
            for tgt in target_nodes:
                if self._nodes_match(src, tgt):
                    self.unified_graph.add_edge(src, tgt, edge_type='cross_system_link')

    def _nodes_match(self, node_a: str, node_b: str) -> bool:
        data_a = self.unified_graph.nodes[node_a].get('metadata', {})
        data_b = self.unified_graph.nodes[node_b].get('metadata', {})
        
        name_a = data_a.get('name', '')
        name_b = data_b.get('name', '')
        
        return name_a and name_b and name_a.lower() == name_b.lower()

    def get_end_to_end_lineage(self, node_id: str, direction: str = 'both') -> nx.DiGraph:
        if node_id not in self.unified_graph:
            return nx.DiGraph()
        
        if direction == 'upstream':
            nodes = nx.ancestors(self.unified_graph, node_id)
        elif direction == 'downstream':
            nodes = nx.descendants(self.unified_graph, node_id)
        else:
            nodes = nx.ancestors(self.unified_graph, node_id) | nx.descendants(self.unified_graph, node_id)
        
        nodes.add(node_id)
        return self.unified_graph.subgraph(nodes).copy()

    def export_graph(self) -> Dict[str, Any]:
        return nx.node_link_data(self.unified_graph)
