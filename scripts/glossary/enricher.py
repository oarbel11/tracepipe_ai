from typing import Dict, Any, Optional
from .manager import GlossaryManager


class CatalogEnricher:
    def __init__(self, glossary_manager: GlossaryManager):
        self.glossary = glossary_manager

    def enrich_asset(self, catalog_path: str, asset_metadata: Dict[str, Any]) -> Dict[str, Any]:
        term = self.glossary.get_term(catalog_path)
        if term:
            enriched = asset_metadata.copy()
            enriched['business_metadata'] = {
                'definition': term.definition,
                'ownership': {
                    'owner': term.ownership.owner,
                    'team': term.ownership.team
                } if term.ownership else None,
                'tags': [{'key': t.key, 'value': t.value} for t in term.tags],
                'pii_status': term.pii_status,
                'quality_score': term.quality_score
            }
            return enriched
        return asset_metadata

    def enrich_lineage(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        enriched = lineage_data.copy()
        if 'nodes' in enriched:
            for node in enriched['nodes']:
                if 'catalog_path' in node:
                    term = self.glossary.get_term(node['catalog_path'])
                    if term:
                        node['business_metadata'] = {
                            'definition': term.definition,
                            'ownership': {
                                'owner': term.ownership.owner,
                                'team': term.ownership.team
                            } if term.ownership else None,
                            'tags': [{'key': t.key, 'value': t.value} for t in term.tags],
                            'pii_status': term.pii_status,
                            'quality_score': term.quality_score
                        }
        return enriched
