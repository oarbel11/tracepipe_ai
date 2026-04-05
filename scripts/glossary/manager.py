"""Business Glossary Manager."""
import json
import os
from typing import Dict, Optional, List
from datetime import datetime


class GlossaryManager:
    """Manages business glossary terms and metadata."""

    def __init__(self, storage_path: str = "data/glossary.json"):
        self.storage_path = storage_path
        self.glossary: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        """Load glossary from JSON file."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    content = f.read().strip()
                    if content:
                        self.glossary = json.loads(content)
                    else:
                        self.glossary = {}
            except (json.JSONDecodeError, IOError):
                self.glossary = {}
        else:
            self.glossary = {}
            self._save()

    def _save(self):
        """Save glossary to JSON file."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, 'w') as f:
            json.dump(self.glossary, f, indent=2)

    def add_term(self, asset_id: str, term: Dict) -> Dict:
        """Add or update a glossary term."""
        term['updated_at'] = datetime.now().isoformat()
        self.glossary[asset_id] = term
        self._save()
        return term

    def get_term(self, asset_id: str) -> Optional[Dict]:
        """Retrieve a glossary term."""
        return self.glossary.get(asset_id)

    def update_term(self, asset_id: str, updates: Dict) -> Optional[Dict]:
        """Update an existing term."""
        if asset_id in self.glossary:
            self.glossary[asset_id].update(updates)
            self.glossary[asset_id]['updated_at'] = datetime.now().isoformat()
            self._save()
            return self.glossary[asset_id]
        return None

    def delete_term(self, asset_id: str) -> bool:
        """Delete a glossary term."""
        if asset_id in self.glossary:
            del self.glossary[asset_id]
            self._save()
            return True
        return False

    def search_terms(self, query: str) -> List[Dict]:
        """Search terms by name or description."""
        results = []
        query_lower = query.lower()
        for asset_id, term in self.glossary.items():
            if (query_lower in term.get('name', '').lower() or
                    query_lower in term.get('description', '').lower()):
                results.append({'asset_id': asset_id, **term})
        return results
