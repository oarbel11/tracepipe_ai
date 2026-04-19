import networkx as nx
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

class LineageUIManager:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.storage = {}
        self.metadata = {}
        self.lineage_id_counter = 0

    def add_lineage(self, source: str, target: str, metadata: Optional[Dict] = None) -> int:
        self.lineage_id_counter += 1
        lineage_id = self.lineage_id_counter
        self.graph.add_edge(source, target)
        edge_data = metadata or {}
        edge_data['lineage_id'] = lineage_id
        edge_data['created_at'] = datetime.now().isoformat()
        self.graph[source][target].update(edge_data)
        self.storage[lineage_id] = {'source': source, 'target': target, 'metadata': edge_data}
        return lineage_id

    def get_lineage(self, node: str, direction: str = 'both') -> Dict[str, Any]:
        result = {'node': node, 'upstream': [], 'downstream': []}
        if node not in self.graph:
            return result
        if direction in ['upstream', 'both']:
            result['upstream'] = list(self.graph.predecessors(node))
        if direction in ['downstream', 'both']:
            result['downstream'] = list(self.graph.successors(node))
        return result

    def get_all_lineage(self) -> List[Dict[str, Any]]:
        return [v for v in self.storage.values()]

    def delete_lineage(self, lineage_id: int) -> bool:
        if lineage_id not in self.storage:
            return False
        edge_info = self.storage[lineage_id]
        if self.graph.has_edge(edge_info['source'], edge_info['target']):
            self.graph.remove_edge(edge_info['source'], edge_info['target'])
        del self.storage[lineage_id]
        return True

    def detect_lineage_issues(self) -> List[Dict[str, Any]]:
        issues = []
        for node in self.graph.nodes():
            if self.graph.in_degree(node) == 0 and self.graph.out_degree(node) == 0:
                issues.append({'type': 'orphan', 'node': node, 'message': f'Orphan node: {node}'})
        try:
            cycles = list(nx.simple_cycles(self.graph))
            for cycle in cycles:
                issues.append({'type': 'cycle', 'nodes': cycle, 'message': f'Cycle detected: {cycle}'})
        except:
            pass
        return issues

    def export_lineage(self) -> str:
        data = {'nodes': list(self.graph.nodes()), 'edges': self.get_all_lineage(), 'metadata': self.metadata}
        return json.dumps(data, indent=2)

    def import_lineage(self, json_data: str) -> bool:
        try:
            data = json.loads(json_data)
            for edge in data.get('edges', []):
                self.add_lineage(edge['source'], edge['target'], edge.get('metadata'))
            self.metadata.update(data.get('metadata', {}))
            return True
        except:
            return False
