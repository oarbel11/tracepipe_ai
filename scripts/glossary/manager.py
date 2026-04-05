import json
import os
from typing import List, Optional
from .models import Term, Ownership, Tag


class GlossaryManager:
    def __init__(self, storage_path: str = '.glossary.json'):
        self.storage_path = storage_path
        self.terms = {}
        self._load()

    def _load(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
                self.terms = {k: Term.from_dict(v) for k, v in data.items()}

    def _save(self):
        with open(self.storage_path, 'w') as f:
            data = {k: v.to_dict() for k, v in self.terms.items()}
            json.dump(data, f, indent=2)

    def add_term(self, term: Term):
        self.terms[term.catalog_path] = term
        self._save()

    def get_term(self, catalog_path: str) -> Optional[Term]:
        return self.terms.get(catalog_path)

    def update_term(self, catalog_path: str, **kwargs):
        if catalog_path in self.terms:
            term = self.terms[catalog_path]
            for key, value in kwargs.items():
                if hasattr(term, key):
                    setattr(term, key, value)
            self._save()
            return term
        return None

    def delete_term(self, catalog_path: str) -> bool:
        if catalog_path in self.terms:
            del self.terms[catalog_path]
            self._save()
            return True
        return False

    def list_terms(self) -> List[Term]:
        return list(self.terms.values())

    def search_terms(self, query: str) -> List[Term]:
        results = []
        query_lower = query.lower()
        for term in self.terms.values():
            if (query_lower in term.name.lower() or
                query_lower in term.definition.lower() or
                query_lower in term.catalog_path.lower()):
                results.append(term)
        return results
