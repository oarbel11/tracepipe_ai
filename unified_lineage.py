from typing import List, Dict, Set, Optional
from dataclasses import dataclass, field


@dataclass
class LineageNode:
    """Represents a node in the lineage graph."""
    id: str
    node_type: str
    name: str
    metadata: Dict = field(default_factory=dict)
    dataframe: Optional[str] = None

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, LineageNode):
            return self.id == other.id
        return False


@dataclass
class ColumnNode(LineageNode):
    """Represents a column node in the lineage graph."""
    column_name: str = ""
    dataframe: Optional[str] = None

    def __post_init__(self):
        if not self.column_name and 'column_name' in self.metadata:
            self.column_name = self.metadata['column_name']


@dataclass
class LineageEdge:
    """Represents an edge in the lineage graph."""
    source: LineageNode
    target: LineageNode
    edge_type: str = "dependency"
    metadata: Dict = field(default_factory=dict)


class LineageGraph:
    """Manages the lineage graph with nodes and edges."""

    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def add_node(self, node: LineageNode) -> None:
        """Add a node to the graph."""
        self.nodes[node.id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge to the graph."""
        self.add_node(edge.source)
        self.add_node(edge.target)
        self.edges.append(edge)

    def get_upstream(self, node: LineageNode) -> List[LineageNode]:
        """Get all upstream nodes recursively."""
        visited: Set[str] = set()
        result: List[LineageNode] = []

        def traverse(current: LineageNode):
            if current.id in visited:
                return
            visited.add(current.id)
            for edge in self.edges:
                if edge.target.id == current.id:
                    result.append(edge.source)
                    traverse(edge.source)

        traverse(node)
        return result

    def get_downstream(self, node: LineageNode) -> List[LineageNode]:
        """Get all downstream nodes recursively."""
        visited: Set[str] = set()
        result: List[LineageNode] = []

        def traverse(current: LineageNode):
            if current.id in visited:
                return
            visited.add(current.id)
            for edge in self.edges:
                if edge.source.id == current.id:
                    result.append(edge.target)
                    traverse(edge.target)

        traverse(node)
        return result
