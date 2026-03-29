import json
import os
from typing import Dict, List, Optional, Any
from pathlib import Path


class MetadataStore:
    def __init__(self, storage_path: str = ".tracepipe/metadata.json"):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if self.storage_path.exists():
            with open(self.storage_path, 'r') as f:
                return json.load(f)
        return {}

    def _save(self) -> None:
        with open(self.storage_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)

    def add_glossary_term(self, entity_id: str, term: str, 
                          definition: str, category: str = "general") -> None:
        if entity_id not in self.metadata:
            self.metadata[entity_id] = {"glossary": [], "owners": [], "quality_rules": []}
        self.metadata[entity_id]["glossary"].append({
            "term": term,
            "definition": definition,
            "category": category
        })
        self._save()

    def add_owner(self, entity_id: str, name: str, 
                  role: str, contact: str) -> None:
        if entity_id not in self.metadata:
            self.metadata[entity_id] = {"glossary": [], "owners": [], "quality_rules": []}
        self.metadata[entity_id]["owners"].append({
            "name": name,
            "role": role,
            "contact": contact
        })
        self._save()

    def add_quality_rule(self, entity_id: str, rule_type: str,
                         description: str, threshold: Optional[float] = None) -> None:
        if entity_id not in self.metadata:
            self.metadata[entity_id] = {"glossary": [], "owners": [], "quality_rules": []}
        rule = {"type": rule_type, "description": description}
        if threshold is not None:
            rule["threshold"] = threshold
        self.metadata[entity_id]["quality_rules"].append(rule)
        self._save()

    def get_metadata(self, entity_id: str) -> Dict[str, Any]:
        return self.metadata.get(entity_id, {
            "glossary": [], "owners": [], "quality_rules": []
        })

    def search_by_term(self, search_term: str) -> List[Dict[str, Any]]:
        results = []
        for entity_id, meta in self.metadata.items():
            for term in meta.get("glossary", []):
                if search_term.lower() in term["term"].lower() or \
                   search_term.lower() in term["definition"].lower():
                    results.append({"entity_id": entity_id, **term})
        return results

    def get_all_entities(self) -> List[str]:
        return list(self.metadata.keys())
