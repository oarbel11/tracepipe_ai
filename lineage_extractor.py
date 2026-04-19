from typing import Dict, List, Optional
from unified_lineage import LineageGraph, LineageNode, LineageEdge, ColumnNode


class LineageExtractor:
    """Extracts lineage from Databricks logical plans."""

    def __init__(self):
        self.graph = LineageGraph()

    def extract_from_plan(self, logical_plan: Dict) -> LineageGraph:
        """Extract lineage from a logical plan."""
        self.graph = LineageGraph()
        self._process_plan(logical_plan)
        return self.graph

    def build_lineage(self, logical_plan: Dict) -> LineageGraph:
        """Build lineage graph from logical plan."""
        return self.extract_from_plan(logical_plan)

    def _process_plan(self, plan: Dict, parent_node: Optional[LineageNode] = None):
        """Recursively process logical plan nodes."""
        if not plan:
            return None

        node_type = plan.get('type', 'unknown')
        node_id = plan.get('id', f"{node_type}_{id(plan)}")
        node_name = plan.get('name', node_id)

        if node_type == 'column':
            node = ColumnNode(
                id=node_id,
                node_type=node_type,
                name=node_name,
                column_name=plan.get('column_name', node_name),
                dataframe=plan.get('dataframe'),
                metadata=plan.get('metadata', {})
            )
        else:
            node = LineageNode(
                id=node_id,
                node_type=node_type,
                name=node_name,
                metadata=plan.get('metadata', {})
            )

        self.graph.add_node(node)

        if parent_node:
            edge = LineageEdge(
                source=node,
                target=parent_node,
                edge_type=plan.get('edge_type', 'dependency')
            )
            self.graph.add_edge(edge)

        for child in plan.get('children', []):
            self._process_plan(child, node)

        for input_node in plan.get('inputs', []):
            self._process_plan(input_node, node)

        return node

    def get_lineage_graph(self) -> LineageGraph:
        """Return the current lineage graph."""
        return self.graph
