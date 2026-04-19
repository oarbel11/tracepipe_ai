import networkx as nx
import json
from typing import Dict, List, Optional, Set, Any
from datetime import datetime

class LineageUIManager:
    def __init__(self):
        self.lineage_graph = nx.DiGraph()
        self.metadata = {}
        self.governance_data = {}
        self.annotations = {}
        self.policies = {}
        self.issues = []

    def add_lineage(self, source: str, target: str, metadata: Dict = None):
        self.lineage_graph.add_edge(source, target)
        if metadata:
            self.metadata[f"{source}->{target}"] = metadata

    def get_upstream(self, node: str, depth: int = -1) -> List[str]:
        if node not in self.lineage_graph:
            return []
        if depth == -1:
            return list(nx.ancestors(self.lineage_graph, node))
        result = set()
        current = {node}
        for _ in range(depth):
            next_level = set()
            for n in current:
                next_level.update(self.lineage_graph.predecessors(n))
            result.update(next_level)
            current = next_level
        return list(result)

    def get_downstream(self, node: str, depth: int = -1) -> List[str]:
        if node not in self.lineage_graph:
            return []
        if depth == -1:
            return list(nx.descendants(self.lineage_graph, node))
        result = set()
        current = {node}
        for _ in range(depth):
            next_level = set()
            for n in current:
                next_level.update(self.lineage_graph.successors(n))
            result.update(next_level)
            current = next_level
        return list(result)

    def analyze_impact(self, node: str, change_type: str) -> Dict:
        downstream = self.get_downstream(node)
        return {"affected_nodes": downstream, "change_type": change_type,
                "impact_count": len(downstream)}

    def add_governance(self, node: str, gov_type: str, value: Any):
        if node not in self.governance_data:
            self.governance_data[node] = {}
        self.governance_data[node][gov_type] = value

    def get_governance(self, node: str) -> Dict:
        return self.governance_data.get(node, {})

    def add_annotation(self, node: str, annotation: str):
        if node not in self.annotations:
            self.annotations[node] = []
        self.annotations[node].append({"text": annotation,
                                       "timestamp": datetime.now().isoformat()})

    def detect_issues(self) -> List[Dict]:
        self.issues = []
        for node in self.lineage_graph.nodes():
            if self.lineage_graph.in_degree(node) == 0 and \
               self.lineage_graph.out_degree(node) == 0:
                self.issues.append({"type": "isolated_node", "node": node})
        return self.issues

    def export_lineage(self) -> str:
        data = {"nodes": list(self.lineage_graph.nodes()),
                "edges": list(self.lineage_graph.edges()),
                "metadata": self.metadata, "governance": self.governance_data}
        return json.dumps(data)
