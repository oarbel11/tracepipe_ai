import os
from typing import Dict, List, Optional
import networkx as nx
from .business_glossary import BusinessGlossary
from .semantic_lineage import SemanticLineageMapper


class ContextBuilder:
    def __init__(self, glossary_path: Optional[str] = None):
        self.glossary = BusinessGlossary(glossary_path or "./glossary.json")
        self.semantic_mapper = SemanticLineageMapper(self.glossary)

    def build_asset_context(self, asset_path: str, lineage_graph: Optional[nx.DiGraph] = None) -> Dict:
        terms = self.glossary.get_terms_for_asset(asset_path)
        
        context = {
            'asset': asset_path,
            'business_terms': terms,
            'has_business_context': len(terms) > 0,
            'business_summary': self._summarize_terms(terms)
        }
        
        if lineage_graph:
            impact = self.semantic_mapper.get_business_impact(asset_path, lineage_graph)
            context['business_impact'] = impact
        
        return context

    def _summarize_terms(self, terms: List[Dict]) -> str:
        if not terms:
            return "No business context defined"
        categories = set(t.get('category', 'General') for t in terms)
        return f"{len(terms)} term(s) across {len(categories)} category(ies)"

    def build_change_context(self, changed_assets: List[str], lineage_graph: nx.DiGraph) -> Dict:
        enriched_graph = self.semantic_mapper.enrich_lineage_graph(lineage_graph)
        
        affected_business_terms = set()
        asset_contexts = []
        
        for asset in changed_assets:
            ctx = self.build_asset_context(asset, enriched_graph)
            asset_contexts.append(ctx)
            if ctx['business_terms']:
                affected_business_terms.update(t['id'] for t in ctx['business_terms'])
        
        return {
            'changed_assets': changed_assets,
            'asset_contexts': asset_contexts,
            'total_affected_terms': len(affected_business_terms),
            'affected_term_ids': list(affected_business_terms),
            'business_readable_summary': self._build_readable_summary(asset_contexts)
        }

    def _build_readable_summary(self, asset_contexts: List[Dict]) -> str:
        total = len(asset_contexts)
        with_context = sum(1 for ctx in asset_contexts if ctx['has_business_context'])
        return f"{with_context}/{total} assets have business context"

    def get_glossary(self) -> BusinessGlossary:
        return self.glossary
