from typing import List, Dict, Set, Optional
from scripts.external_connectors import LineageNode, LineageEdge
import re

class LineageStitcher:
    def __init__(self):
        self.match_rules = []
        self._setup_default_rules()

    def _setup_default_rules(self):
        self.match_rules = [
            (r'databricks\.(?P<catalog>\w+)\.(?P<schema>\w+)\.(?P<table>\w+)', 'unity_catalog'),
            (r'dbt\.(?P<database>\w+)\.(?P<schema>\w+)\.(?P<name>\w+)', 'dbt'),
            (r'tableau\.(?P<connection>[\w\.]+)', 'tableau'),
            (r'salesforce\.(?P<object>\w+)', 'salesforce')
        ]

    def extract_identifiers(self, node: LineageNode) -> Set[str]:
        identifiers = {node.identifier}
        if node.metadata:
            if 'database' in node.metadata and 'schema' in node.metadata and 'name' in node.metadata:
                identifiers.add(f"{node.metadata['database']}.{node.metadata['schema']}.{node.metadata['name']}")
            if 'connection' in node.metadata:
                identifiers.add(node.metadata['connection'])
        return identifiers

    def find_matches(self, node1: LineageNode, node2: LineageNode) -> bool:
        ids1 = self.extract_identifiers(node1)
        ids2 = self.extract_identifiers(node2)
        
        for id1 in ids1:
            for id2 in ids2:
                if self._identifiers_match(id1, id2):
                    return True
        return False

    def _identifiers_match(self, id1: str, id2: str) -> bool:
        parts1 = id1.lower().split('.')
        parts2 = id2.lower().split('.')
        common = set(parts1) & set(parts2)
        return len(common) >= 2

    def stitch_lineage(self, all_nodes: List[LineageNode], all_edges: List[LineageEdge]) -> List[LineageEdge]:
        stitched_edges = list(all_edges)
        node_map = {}
        
        for i, node1 in enumerate(all_nodes):
            for node2 in all_nodes[i+1:]:
                if node1.system != node2.system and self.find_matches(node1, node2):
                    stitched_edges.append(LineageEdge(
                        source=node1,
                        target=node2,
                        edge_type="cross_system_link"
                    ))
        
        return stitched_edges

    def create_unified_identifier(self, node: LineageNode) -> str:
        if node.metadata:
            parts = [node.metadata.get('database', ''), node.metadata.get('schema', ''), node.metadata.get('name', '')]
            return '.'.join(filter(None, parts)) or node.identifier
        return node.identifier
