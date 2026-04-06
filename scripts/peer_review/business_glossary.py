from typing import Dict, List, Optional
from dataclasses import dataclass, field
import json
from datetime import datetime

@dataclass
class BusinessTerm:
    term_id: str
    name: str
    definition: str
    category: str = "general"
    synonyms: List[str] = field(default_factory=list)
    related_terms: List[str] = field(default_factory=list)
    owners: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict:
        return {
            "term_id": self.term_id,
            "name": self.name,
            "definition": self.definition,
            "category": self.category,
            "synonyms": self.synonyms,
            "related_terms": self.related_terms,
            "owners": self.owners,
            "tags": self.tags,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'BusinessTerm':
        return cls(**data)

class BusinessGlossary:
    def __init__(self):
        self.terms: Dict[str, BusinessTerm] = {}

    def add_term(self, term: BusinessTerm) -> None:
        self.terms[term.term_id] = term

    def get_term(self, term_id: str) -> Optional[BusinessTerm]:
        return self.terms.get(term_id)

    def search_terms(self, query: str) -> List[BusinessTerm]:
        query_lower = query.lower()
        results = []
        for term in self.terms.values():
            if (query_lower in term.name.lower() or 
                query_lower in term.definition.lower() or
                any(query_lower in syn.lower() for syn in term.synonyms)):
                results.append(term)
        return results

    def get_terms_by_category(self, category: str) -> List[BusinessTerm]:
        return [t for t in self.terms.values() if t.category == category]

    def export_glossary(self) -> str:
        data = {tid: term.to_dict() for tid, term in self.terms.items()}
        return json.dumps(data, indent=2)

    def import_glossary(self, json_str: str) -> None:
        data = json.loads(json_str)
        for term_id, term_data in data.items():
            self.terms[term_id] = BusinessTerm.from_dict(term_data)
