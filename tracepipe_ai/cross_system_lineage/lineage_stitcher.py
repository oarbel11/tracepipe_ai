from typing import Dict, List, Any, Set, Tuple


class LineageStitcher:
    """Stitches together lineage from multiple systems."""

    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Tuple[str, str]] = []

    def add_unity_catalog_lineage(self, lineage: List[Dict[str, Any]]) -> None:
        """Add Unity Catalog lineage data."""
        for item in lineage:
            source = item.get('source')
            target = item.get('target')
            if source and target:
                self._add_node(source, item.get('source_metadata', {}))
                self._add_node(target, item.get('target_metadata', {}))
                self.edges.append((source, target))

    def add_external_lineage(self, system_name: str,
                             lineage: List[Dict[str, Any]]) -> None:
        """Add external system lineage data."""
        for item in lineage:
            source = f"{system_name}:{item.get('source')}"
            target = f"{system_name}:{item.get('target')}"
            self._add_node(source, item.get('source_metadata', {}))
            self._add_node(target, item.get('target_metadata', {}))
            self.edges.append((source, target))

    def stitch_lineage(self, matching_rules: List[Dict[str, Any]]
                       ) -> Dict[str, Any]:
        """Stitch together lineage using matching rules."""
        for rule in matching_rules:
            source_pattern = rule.get('source_pattern')
            target_pattern = rule.get('target_pattern')
            if source_pattern and target_pattern:
                self._apply_matching_rule(source_pattern, target_pattern)

        return self.get_complete_lineage()

    def get_complete_lineage(self) -> Dict[str, Any]:
        """Get the complete stitched lineage."""
        return {
            'nodes': list(self.nodes.values()),
            'edges': [{'source': s, 'target': t} for s, t in self.edges]
        }

    def _add_node(self, node_id: str, metadata: Dict[str, Any]) -> None:
        """Add a node to the lineage graph."""
        if node_id not in self.nodes:
            self.nodes[node_id] = {'id': node_id, 'metadata': metadata}

    def _apply_matching_rule(self, source_pattern: str,
                             target_pattern: str) -> None:
        """Apply matching rules to connect nodes across systems."""
        matching_nodes = []
        for node_id in self.nodes:
            if source_pattern in node_id or target_pattern in node_id:
                matching_nodes.append(node_id)

        for i in range(len(matching_nodes) - 1):
            self.edges.append((matching_nodes[i], matching_nodes[i + 1]))

    def get_downstream(self, node_id: str) -> List[str]:
        """Get all downstream nodes from a given node."""
        downstream = []
        for source, target in self.edges:
            if source == node_id:
                downstream.append(target)
        return downstream

    def get_upstream(self, node_id: str) -> List[str]:
        """Get all upstream nodes from a given node."""
        upstream = []
        for source, target in self.edges:
            if target == node_id:
                upstream.append(source)
        return upstream
