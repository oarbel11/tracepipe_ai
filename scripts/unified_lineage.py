"""Unified lineage graph builder and query interface."""
from typing import Dict, List, Optional
from scripts.lineage_stitcher import LineageStitcher
from scripts.external_connectors import DbtConnector, TableauConnector, SalesforceConnector


class UnifiedLineageGraph:
    """Represents unified lineage graph."""

    def __init__(self):
        self.entities = {}
        self.edges = []

    def add_entity(self, entity: Dict):
        """Add entity to graph."""
        eid = entity.get('id')
        if eid:
            self.entities[eid] = entity

    def add_edge(self, from_id: str, to_id: str):
        """Add edge to graph."""
        self.edges.append({'from': from_id, 'to': to_id})

    def get_upstream(self, entity_id: str) -> List[str]:
        """Get upstream dependencies."""
        return [e['from'] for e in self.edges if e['to'] == entity_id]

    def get_downstream(self, entity_id: str) -> List[str]:
        """Get downstream dependencies."""
        return [e['to'] for e in self.edges if e['from'] == entity_id]

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'entities': list(self.entities.values()),
            'edges': self.edges
        }


class UnifiedLineageBuilder:
    """Builds unified lineage from multiple sources."""

    def __init__(self):
        self.stitcher = LineageStitcher()
        self.connectors = {}

    def add_connector(self, name: str, connector):
        """Register a connector."""
        self.connectors[name] = connector

    def build(self, unity_catalog_lineage: Optional[Dict] = None) -> UnifiedLineageGraph:
        """Build unified lineage graph."""
        lineages = []

        if unity_catalog_lineage:
            lineages.append(unity_catalog_lineage)

        for name, connector in self.connectors.items():
            try:
                lineage = connector.extract_lineage()
                lineages.append(lineage)
            except:
                pass

        merged = self.stitcher.merge_lineage(lineages)
        graph = UnifiedLineageGraph()

        for entity in merged.get('entities', []):
            graph.add_entity(entity)

        for edge in merged.get('edges', []):
            graph.add_edge(edge.get('from'), edge.get('to'))

        return graph
