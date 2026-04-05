from typing import Dict, List, Set
import networkx as nx
from scripts.business_glossary import GlossaryManager

class SemanticLineageBuilder:
    def __init__(self, glossary: GlossaryManager):
        self.glossary = glossary
        self.lineage_graph = nx.DiGraph()

    def add_technical_lineage(self, source: str, target: str, lineage_type: str = 'derived_from'):
        self.lineage_graph.add_edge(source, target, type=lineage_type)

    def get_semantic_lineage(self, asset_path: str, depth: int = 3) -> Dict:
        if asset_path not in self.lineage_graph:
            self.lineage_graph.add_node(asset_path)

        upstream = self._get_neighbors(asset_path, direction='upstream', depth=depth)
        downstream = self._get_neighbors(asset_path, direction='downstream', depth=depth)

        result = {
            'asset': asset_path,
            'business_terms': self.glossary.get_terms_for_asset(asset_path),
            'upstream': self._enrich_with_terms(upstream),
            'downstream': self._enrich_with_terms(downstream),
            'business_impact': self._calculate_business_impact(asset_path, downstream)
        }
        return result

    def _get_neighbors(self, node: str, direction: str, depth: int) -> List[str]:
        if node not in self.lineage_graph:
            return []
        neighbors = set()
        if direction == 'upstream':
            for d in range(1, depth + 1):
                for predecessor in self.lineage_graph.predecessors(node):
                    neighbors.add(predecessor)
                    neighbors.update(self._get_neighbors(predecessor, direction, d - 1))
        else:
            for d in range(1, depth + 1):
                for successor in self.lineage_graph.successors(node):
                    neighbors.add(successor)
                    neighbors.update(self._get_neighbors(successor, direction, d - 1))
        return list(neighbors)

    def _enrich_with_terms(self, assets: List[str]) -> List[Dict]:
        enriched = []
        for asset in assets:
            terms = self.glossary.get_terms_for_asset(asset)
            enriched.append({
                'asset': asset,
                'business_terms': terms,
                'has_business_context': len(terms) > 0
            })
        return enriched

    def _calculate_business_impact(self, asset: str, downstream: List[str]) -> Dict:
        total_downstream = len(downstream)
        with_context = sum(1 for d in downstream
                          if self.glossary.get_terms_for_asset(d))
        categories = set()
        for d in downstream:
            for term in self.glossary.get_terms_for_asset(d):
                if term.get('category'):
                    categories.add(term['category'])
        return {
            'affected_assets': total_downstream,
            'business_contextualized': with_context,
            'affected_categories': list(categories)
        }
