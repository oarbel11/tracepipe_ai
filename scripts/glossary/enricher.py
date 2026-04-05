"""Catalog Enricher for Unity Catalog integration."""
from typing import Dict, Optional, List
from .manager import GlossaryManager


class CatalogEnricher:
    """Enriches Unity Catalog assets with business metadata."""

    def __init__(self, glossary_manager: Optional[GlossaryManager] = None):
        self.glossary = glossary_manager or GlossaryManager()

    def enrich_asset(self, asset: Dict, metadata: Dict) -> Dict:
        """Enrich a catalog asset with business metadata."""
        asset_id = asset.get('id') or asset.get('name')
        if not asset_id:
            raise ValueError("Asset must have 'id' or 'name'")

        enriched = asset.copy()
        term_data = {
            'name': metadata.get('name', asset.get('name')),
            'description': metadata.get('description', ''),
            'owner': metadata.get('owner'),
            'tags': metadata.get('tags', []),
            'pii_status': metadata.get('pii_status', False),
            'quality_score': metadata.get('quality_score')
        }

        self.glossary.add_term(asset_id, term_data)
        enriched['business_metadata'] = term_data
        return enriched

    def get_enriched_asset(self, asset_id: str) -> Optional[Dict]:
        """Get enriched metadata for an asset."""
        return self.glossary.get_term(asset_id)

    def bulk_enrich(self, assets: List[Dict],
                    metadata_map: Dict[str, Dict]) -> List[Dict]:
        """Enrich multiple assets at once."""
        enriched_assets = []
        for asset in assets:
            asset_id = asset.get('id') or asset.get('name')
            if asset_id and asset_id in metadata_map:
                enriched = self.enrich_asset(asset, metadata_map[asset_id])
                enriched_assets.append(enriched)
            else:
                enriched_assets.append(asset)
        return enriched_assets

    def add_quality_metrics(self, asset_id: str, metrics: Dict) -> bool:
        """Add data quality metrics to an asset."""
        term = self.glossary.get_term(asset_id)
        if term:
            term['quality_metrics'] = metrics
            self.glossary.update_term(asset_id, term)
            return True
        return False

    def set_ownership(self, asset_id: str, owner: str) -> bool:
        """Set ownership for an asset."""
        return self.glossary.update_term(asset_id, {'owner': owner}) is not None

    def add_tags(self, asset_id: str, tags: List[str]) -> bool:
        """Add tags to an asset."""
        term = self.glossary.get_term(asset_id)
        if term:
            existing_tags = set(term.get('tags', []))
            existing_tags.update(tags)
            return self.glossary.update_term(
                asset_id, {'tags': list(existing_tags)}
            ) is not None
        return False
