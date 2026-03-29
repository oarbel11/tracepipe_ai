from typing import Dict, List, Optional, Set
import networkx as nx
from .business_glossary import BusinessGlossary


class SemanticLineageMapper:
    def __init__(self, glossary: BusinessGlossary):
        self.glossary = glossary

    def enrich_lineage_graph(self, lineage_graph: nx.DiGraph) -> nx.DiGraph:
        enriched = lineage_graph.copy()
        for node in enriched.nodes():
            terms = self.glossary.get_terms_for_asset(node)
            if terms:
                enriched.nodes[node]['business_terms'] = terms
                enriched.nodes[node]['business_context'] = self._build_context(terms)
        return enriched

    def _build_context(self, terms: List[Dict]) -> str:
        if not terms:
            return ""
        context_parts = [f"{t['name']}: {t['definition']}" for t in terms]
        return " | ".join(context_parts)

    def get_business_impact(self, asset_path: str, lineage_graph: nx.DiGraph) -> Dict:
        downstream = nx.descendants(lineage_graph, asset_path) if asset_path in lineage_graph else set()
        upstream = nx.ancestors(lineage_graph, asset_path) if asset_path in lineage_graph else set()
        
        affected_terms = set()
        affected_assets_with_terms = []
        
        for node in downstream | upstream | {asset_path}:
            terms = self.glossary.get_terms_for_asset(node)
            if terms:
                affected_assets_with_terms.append({
                    'asset': node,
                    'terms': terms
                })
                affected_terms.update(t['id'] for t in terms)
        
        return {
            'asset': asset_path,
            'affected_term_count': len(affected_terms),
            'affected_terms': list(affected_terms),
            'assets_with_business_context': affected_assets_with_terms,
            'downstream_count': len(downstream),
            'upstream_count': len(upstream)
        }

    def get_semantic_path(self, source: str, target: str, lineage_graph: nx.DiGraph) -> List[Dict]:
        if source not in lineage_graph or target not in lineage_graph:
            return []
        
        try:
            path = nx.shortest_path(lineage_graph, source, target)
            semantic_path = []
            for node in path:
                terms = self.glossary.get_terms_for_asset(node)
                semantic_path.append({
                    'asset': node,
                    'business_terms': terms,
                    'business_readable': self._build_context(terms) or node
                })
            return semantic_path
        except nx.NetworkXNoPath:
            return []
