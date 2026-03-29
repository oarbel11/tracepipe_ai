from typing import Dict, List, Any, Optional
import networkx as nx
from scripts.metadata_store import MetadataStore


class LineageEnricher:
    def __init__(self, metadata_store: Optional[MetadataStore] = None):
        self.metadata_store = metadata_store or MetadataStore()

    def enrich_graph(self, lineage_graph: nx.DiGraph) -> nx.DiGraph:
        enriched = lineage_graph.copy()
        for node in enriched.nodes():
            metadata = self.metadata_store.get_metadata(node)
            enriched.nodes[node]['metadata'] = metadata
            enriched.nodes[node]['has_context'] = self._has_context(metadata)
        return enriched

    def _has_context(self, metadata: Dict[str, Any]) -> bool:
        return bool(metadata.get('glossary') or 
                   metadata.get('owners') or 
                   metadata.get('quality_rules'))

    def get_enriched_node(self, lineage_graph: nx.DiGraph, 
                         node_id: str) -> Dict[str, Any]:
        if node_id not in lineage_graph.nodes():
            raise ValueError(f"Node {node_id} not found in lineage graph")
        
        node_data = dict(lineage_graph.nodes[node_id])
        metadata = self.metadata_store.get_metadata(node_id)
        
        return {
            "id": node_id,
            "node_data": node_data,
            "business_context": metadata
        }

    def get_context_summary(self, lineage_graph: nx.DiGraph) -> Dict[str, Any]:
        total_nodes = lineage_graph.number_of_nodes()
        nodes_with_context = sum(
            1 for node in lineage_graph.nodes()
            if self._has_context(self.metadata_store.get_metadata(node))
        )
        
        return {
            "total_nodes": total_nodes,
            "nodes_with_context": nodes_with_context,
            "coverage_percentage": (nodes_with_context / total_nodes * 100) 
                                   if total_nodes > 0 else 0,
            "entities_by_owner": self._group_by_owners(),
            "terms_by_category": self._group_by_category()
        }

    def _group_by_owners(self) -> Dict[str, int]:
        owner_counts = {}
        for entity_id in self.metadata_store.get_all_entities():
            meta = self.metadata_store.get_metadata(entity_id)
            for owner in meta.get('owners', []):
                name = owner['name']
                owner_counts[name] = owner_counts.get(name, 0) + 1
        return owner_counts

    def _group_by_category(self) -> Dict[str, int]:
        category_counts = {}
        for entity_id in self.metadata_store.get_all_entities():
            meta = self.metadata_store.get_metadata(entity_id)
            for term in meta.get('glossary', []):
                cat = term['category']
                category_counts[cat] = category_counts.get(cat, 0) + 1
        return category_counts
