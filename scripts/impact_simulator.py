import json
from typing import Dict, List, Set, Any
from pathlib import Path


class ImpactSimulator:
    def __init__(self, lineage_data: Dict[str, Any]):
        self.lineage_data = lineage_data
        self.graph = self._build_graph()

    def _build_graph(self) -> Dict[str, Set[str]]:
        graph = {}
        for node_id, node_data in self.lineage_data.get('nodes', {}).items():
            graph[node_id] = set()
        for edge in self.lineage_data.get('edges', []):
            source = edge.get('source')
            target = edge.get('target')
            if source and target:
                if source not in graph:
                    graph[source] = set()
                graph[source].add(target)
        return graph

    def simulate_change(self, node_id: str, change_type: str) -> Dict[str, Any]:
        affected = self._find_downstream(node_id)
        impact_details = []
        for affected_node in affected:
            node_info = self.lineage_data.get('nodes', {}).get(affected_node, {})
            impact_details.append({
                'node_id': affected_node,
                'type': node_info.get('type', 'unknown'),
                'name': node_info.get('name', affected_node),
                'impact_severity': self._calculate_severity(affected_node, change_type)
            })
        return {
            'change_node': node_id,
            'change_type': change_type,
            'affected_count': len(affected),
            'affected_nodes': affected,
            'impact_details': impact_details
        }

    def _find_downstream(self, node_id: str) -> List[str]:
        visited = set()
        queue = [node_id]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for neighbor in self.graph.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)
        visited.discard(node_id)
        return sorted(list(visited))

    def _calculate_severity(self, node_id: str, change_type: str) -> str:
        node_info = self.lineage_data.get('nodes', {}).get(node_id, {})
        node_type = node_info.get('type', '')
        if change_type == 'schema_change':
            if node_type in ['dashboard', 'report']:
                return 'high'
            elif node_type == 'ml_model':
                return 'critical'
        elif change_type == 'deprecation':
            return 'critical'
        return 'medium'

    def get_impact_graph(self, node_id: str) -> Dict[str, Any]:
        affected = self._find_downstream(node_id)
        nodes = {node_id: self.lineage_data.get('nodes', {}).get(node_id, {})}
        for affected_node in affected:
            nodes[affected_node] = self.lineage_data.get('nodes', {}).get(
                affected_node, {})
        edges = []
        for edge in self.lineage_data.get('edges', []):
            if edge['source'] in nodes and edge['target'] in nodes:
                edges.append(edge)
        return {'nodes': nodes, 'edges': edges}
