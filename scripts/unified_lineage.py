import networkx as nx
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field
from enum import Enum

class NodeType(Enum):
    UC_TABLE = "unity_catalog_table"
    UC_VOLUME = "unity_catalog_volume"
    EXTERNAL_TABLE = "external_table"
    BI_REPORT = "bi_report"
    ORCHESTRATION = "orchestration"
    NOTEBOOK = "notebook"

@dataclass
class LineageNode:
    id: str
    name: str
    node_type: NodeType
    metadata: Dict = field(default_factory=dict)

@dataclass
class LineageEdge:
    source: str
    target: str
    edge_type: str = "direct"
    metadata: Dict = field(default_factory=dict)

class UnifiedLineageGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, LineageNode] = {}
    
    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.id] = node
        self.graph.add_node(node.id, **node.metadata)
    
    def add_edge(self, edge: LineageEdge) -> None:
        self.graph.add_edge(edge.source, edge.target, 
                           edge_type=edge.edge_type, **edge.metadata)
    
    def get_upstream(self, node_id: str, max_depth: Optional[int] = None) -> Set[str]:
        if node_id not in self.graph:
            return set()
        upstream = set()
        if max_depth is None:
            upstream = nx.ancestors(self.graph, node_id)
        else:
            for depth in range(1, max_depth + 1):
                for node in list(upstream) + [node_id]:
                    upstream.update(self.graph.predecessors(node))
        return upstream
    
    def get_downstream(self, node_id: str, max_depth: Optional[int] = None) -> Set[str]:
        if node_id not in self.graph:
            return set()
        downstream = set()
        if max_depth is None:
            downstream = nx.descendants(self.graph, node_id)
        else:
            for depth in range(1, max_depth + 1):
                for node in list(downstream) + [node_id]:
                    downstream.update(self.graph.successors(node))
        return downstream
    
    def get_impact_analysis(self, node_id: str) -> Dict:
        return {
            "node": self.nodes.get(node_id),
            "upstream_count": len(self.get_upstream(node_id)),
            "downstream_count": len(self.get_downstream(node_id)),
            "downstream_nodes": list(self.get_downstream(node_id))
        }
    
    def merge_from_unity_catalog(self, uc_lineage: Dict) -> None:
        for table in uc_lineage.get("tables", []):
            node = LineageNode(table["id"], table["name"], 
                             NodeType.UC_TABLE, table.get("metadata", {}))
            self.add_node(node)
        for edge in uc_lineage.get("edges", []):
            self.add_edge(LineageEdge(edge["source"], edge["target"]))
