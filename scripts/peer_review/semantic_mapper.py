from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from scripts.peer_review.business_glossary import BusinessGlossary, BusinessTerm
import json

@dataclass
class SemanticLink:
    term_id: str
    asset_type: str
    asset_id: str
    confidence: float = 1.0
    metadata: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "term_id": self.term_id,
            "asset_type": self.asset_type,
            "asset_id": self.asset_id,
            "confidence": self.confidence,
            "metadata": self.metadata
        }

class SemanticMapper:
    def __init__(self):
        self.glossary = BusinessGlossary()
        self.links: List[SemanticLink] = []
        self.asset_to_terms: Dict[str, Set[str]] = {}
        self.term_to_assets: Dict[str, Set[str]] = {}

    def add_business_term(self, term_id: str, name: str, definition: str,
                          category: str = "general", synonyms: List[str] = None,
                          owners: List[str] = None, tags: List[str] = None) -> BusinessTerm:
        term = BusinessTerm(
            term_id=term_id,
            name=name,
            definition=definition,
            category=category,
            synonyms=synonyms or [],
            owners=owners or [],
            tags=tags or []
        )
        self.glossary.add_term(term)
        return term

    def link_term_to_asset(self, term_id: str, asset_type: str, asset_id: str,
                           confidence: float = 1.0, metadata: Dict = None) -> None:
        link = SemanticLink(term_id, asset_type, asset_id, confidence, metadata or {})
        self.links.append(link)
        
        if asset_id not in self.asset_to_terms:
            self.asset_to_terms[asset_id] = set()
        self.asset_to_terms[asset_id].add(term_id)
        
        if term_id not in self.term_to_assets:
            self.term_to_assets[term_id] = set()
        self.term_to_assets[term_id].add(asset_id)

    def get_terms_for_asset(self, asset_id: str) -> List[BusinessTerm]:
        term_ids = self.asset_to_terms.get(asset_id, set())
        return [self.glossary.get_term(tid) for tid in term_ids if self.glossary.get_term(tid)]

    def get_assets_for_term(self, term_id: str) -> List[Dict]:
        asset_ids = self.term_to_assets.get(term_id, set())
        return [{"asset_id": aid, "links": [l.to_dict() for l in self.links if l.asset_id == aid and l.term_id == term_id]} for aid in asset_ids]

    def search_by_business_term(self, query: str) -> List[Dict]:
        matching_terms = self.glossary.search_terms(query)
        results = []
        for term in matching_terms:
            assets = self.get_assets_for_term(term.term_id)
            for asset in assets:
                results.append({
                    "term": term.to_dict(),
                    "asset_id": asset["asset_id"],
                    "name": term.name,
                    "path": asset["asset_id"]
                })
        return results
