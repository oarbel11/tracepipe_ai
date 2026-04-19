"""Manages governance features."""
from typing import Dict, List, Optional


class GovernanceManager:
    """Manages data governance policies and classifications."""

    def __init__(self):
        self.classifications = {}
        self.glossary = {}
        self.masking_policies = {}
        self.tags = {}

    def add_classification(self, entity: str, column: str,
                          classification: str) -> bool:
        """Add data classification to column."""
        key = f"{entity}.{column}"
        self.classifications[key] = classification
        return True

    def get_classification(self, entity: str, column: str) -> Optional[str]:
        """Get classification for column."""
        key = f"{entity}.{column}"
        return self.classifications.get(key)

    def add_glossary_term(self, term: str, definition: str,
                         metadata: Dict = None) -> bool:
        """Add business glossary term."""
        self.glossary[term] = {
            "definition": definition,
            "metadata": metadata or {}
        }
        return True

    def get_glossary_term(self, term: str) -> Optional[Dict]:
        """Get glossary term definition."""
        return self.glossary.get(term)

    def apply_masking_policy(self, entity: str, column: str,
                            policy: str) -> bool:
        """Apply data masking policy to column."""
        key = f"{entity}.{column}"
        self.masking_policies[key] = policy
        return True

    def get_masking_policy(self, entity: str, column: str) -> Optional[str]:
        """Get masking policy for column."""
        key = f"{entity}.{column}"
        return self.masking_policies.get(key)

    def add_tag(self, entity: str, tag: str) -> bool:
        """Add tag to entity."""
        if entity not in self.tags:
            self.tags[entity] = []
        if tag not in self.tags[entity]:
            self.tags[entity].append(tag)
        return True

    def get_tags(self, entity: str) -> List[str]:
        """Get tags for entity."""
        return self.tags.get(entity, [])

    def get_entities_by_classification(self, classification: str) -> List[str]:
        """Get entities with specific classification."""
        result = []
        for key, cls in self.classifications.items():
            if cls == classification:
                result.append(key)
        return result

    def get_governance_summary(self, entity: str) -> Dict:
        """Get governance summary for entity."""
        classifications = {}
        masking = {}
        for key in self.classifications:
            if key.startswith(f"{entity}."):
                col = key.split(".", 1)[1]
                classifications[col] = self.classifications[key]
        for key in self.masking_policies:
            if key.startswith(f"{entity}."):
                col = key.split(".", 1)[1]
                masking[col] = self.masking_policies[key]
        return {
            "entity": entity,
            "classifications": classifications,
            "masking_policies": masking,
            "tags": self.get_tags(entity)
        }
