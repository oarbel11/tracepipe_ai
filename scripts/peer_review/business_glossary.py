import json
import os
from typing import Dict, List, Optional, Set
from datetime import datetime


class BusinessGlossary:
    def __init__(self, storage_path: str = "./glossary.json"):
        self.storage_path = storage_path
        self.terms: Dict[str, Dict] = {}
        self.asset_mappings: Dict[str, Set[str]] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
                self.terms = data.get('terms', {})
                self.asset_mappings = {
                    k: set(v) for k, v in data.get('asset_mappings', {}).items()
                }

    def _save(self):
        with open(self.storage_path, 'w') as f:
            json.dump({
                'terms': self.terms,
                'asset_mappings': {k: list(v) for k, v in self.asset_mappings.items()}
            }, f, indent=2)

    def add_term(self, term_id: str, name: str, definition: str,
                 owner: str = None, category: str = None, tags: List[str] = None):
        self.terms[term_id] = {
            'name': name,
            'definition': definition,
            'owner': owner,
            'category': category,
            'tags': tags or [],
            'created_at': datetime.utcnow().isoformat()
        }
        self._save()

    def link_asset(self, term_id: str, asset_path: str):
        if term_id not in self.terms:
            raise ValueError(f"Term {term_id} not found")
        if asset_path not in self.asset_mappings:
            self.asset_mappings[asset_path] = set()
        self.asset_mappings[asset_path].add(term_id)
        self._save()

    def get_terms_for_asset(self, asset_path: str) -> List[Dict]:
        term_ids = self.asset_mappings.get(asset_path, set())
        return [{'id': tid, **self.terms[tid]} for tid in term_ids if tid in self.terms]

    def get_assets_for_term(self, term_id: str) -> List[str]:
        return [asset for asset, terms in self.asset_mappings.items() if term_id in terms]

    def search_terms(self, query: str) -> List[Dict]:
        results = []
        query_lower = query.lower()
        for tid, term in self.terms.items():
            if (query_lower in term['name'].lower() or
                query_lower in term['definition'].lower() or
                any(query_lower in tag.lower() for tag in term.get('tags', []))):
                results.append({'id': tid, **term})
        return results
