import json
from typing import Dict, List, Any
from pathlib import Path


class BlastRadiusAnalyzer:
    def __init__(self, lineage_file: str):
        self.lineage_file = lineage_file
        self.lineage_data = self._load_lineage()

    def _load_lineage(self) -> Dict[str, Any]:
        path = Path(self.lineage_file)
        if path.exists():
            with open(path, 'r') as f:
                return json.load(f)
        return {'nodes': {}, 'edges': []}

    def analyze(self, node_id: str) -> Dict[str, Any]:
        downstream = self._find_downstream_nodes(node_id)
        return {
            'source_node': node_id,
            'affected_nodes': downstream,
            'blast_radius': len(downstream)
        }

    def _find_downstream_nodes(self, node_id: str) -> List[str]:
        graph = {}
        for edge in self.lineage_data.get('edges', []):
            source = edge.get('source')
            target = edge.get('target')
            if source not in graph:
                graph[source] = []
            graph[source].append(target)
        visited = set()
        queue = [node_id]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)
        visited.discard(node_id)
        return sorted(list(visited))

    def simulate_scenario(self, node_id: str, scenario_type: str) -> Dict[str, Any]:
        affected = self._find_downstream_nodes(node_id)
        return {
            'scenario': scenario_type,
            'target_node': node_id,
            'affected_count': len(affected),
            'affected_nodes': affected,
            'risk_level': 'high' if len(affected) > 5 else 'medium'
        }
