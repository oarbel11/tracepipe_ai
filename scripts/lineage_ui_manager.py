import networkx as nx
from typing import Dict, List, Any, Optional
import json

class LineageUIManager:
    def __init__(self, workspace_client=None):
        self.workspace_client = workspace_client
        self.lineage_graph = nx.DiGraph()
        self.annotations = {}
        self.policies = {}
        self.classifications = {}
        self.glossary_terms = {}
        self.masking_policies = {}

    def add_lineage_node(self, node_id: str, node_type: str, metadata: Dict):
        self.lineage_graph.add_node(node_id, type=node_type, **metadata)

    def add_lineage_edge(self, source: str, target: str, transform: str = ""):
        self.lineage_graph.add_edge(source, target, transform=transform)

    def get_lineage_graph(self, node_id: str, depth: int = 5) -> Dict:
        if node_id not in self.lineage_graph:
            return {"nodes": [], "edges": []}
        
        subgraph_nodes = set([node_id])
        for _ in range(depth):
            new_nodes = set()
            for n in subgraph_nodes:
                new_nodes.update(self.lineage_graph.predecessors(n))
                new_nodes.update(self.lineage_graph.successors(n))
            subgraph_nodes.update(new_nodes)
        
        subgraph = self.lineage_graph.subgraph(subgraph_nodes)
        nodes = [{"id": n, **self.lineage_graph.nodes[n]} for n in subgraph.nodes()]
        edges = [{"source": u, "target": v, **subgraph.edges[u, v]} for u, v in subgraph.edges()]
        return {"nodes": nodes, "edges": edges}

    def impact_analysis(self, node_id: str, change_type: str) -> Dict:
        if node_id not in self.lineage_graph:
            return {"impacted_nodes": [], "risk_level": "none"}
        
        downstream = list(nx.descendants(self.lineage_graph, node_id))
        risk = "high" if len(downstream) > 10 else "medium" if len(downstream) > 5 else "low"
        return {"impacted_nodes": downstream, "risk_level": risk, "change_type": change_type}

    def add_classification(self, node_id: str, classification: str):
        self.classifications[node_id] = classification

    def add_glossary_term(self, node_id: str, term: str, definition: str):
        self.glossary_terms[node_id] = {"term": term, "definition": definition}

    def add_masking_policy(self, node_id: str, policy: str):
        self.masking_policies[node_id] = policy

    def get_governance_info(self, node_id: str) -> Dict:
        return {
            "classification": self.classifications.get(node_id),
            "glossary_term": self.glossary_terms.get(node_id),
            "masking_policy": self.masking_policies.get(node_id)
        }

    def detect_lineage_issues(self) -> List[Dict]:
        issues = []
        for node in self.lineage_graph.nodes():
            if self.lineage_graph.in_degree(node) == 0 and self.lineage_graph.out_degree(node) == 0:
                issues.append({"node": node, "issue": "isolated_node", "severity": "warning"})
        return issues

    def export_lineage(self, format: str = "json") -> str:
        graph_data = nx.node_link_data(self.lineage_graph)
        return json.dumps(graph_data, indent=2)
