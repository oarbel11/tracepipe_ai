from typing import Dict, List, Any, Optional
import json

class GovernanceUIManager:
    def __init__(self):
        self.policies = {}
        self.tags = {}
        self.classifications = {}
        self.glossary = {}
        self.policy_id_counter = 0

    def add_policy(self, name: str, policy_type: str, config: Dict[str, Any]) -> int:
        self.policy_id_counter += 1
        policy_id = self.policy_id_counter
        self.policies[policy_id] = {
            'id': policy_id,
            'name': name,
            'type': policy_type,
            'config': config,
            'enabled': True
        }
        return policy_id

    def get_policy(self, policy_id: int) -> Optional[Dict[str, Any]]:
        return self.policies.get(policy_id)

    def get_all_policies(self) -> List[Dict[str, Any]]:
        return list(self.policies.values())

    def update_policy(self, policy_id: int, updates: Dict[str, Any]) -> bool:
        if policy_id not in self.policies:
            return False
        self.policies[policy_id].update(updates)
        return True

    def delete_policy(self, policy_id: int) -> bool:
        if policy_id not in self.policies:
            return False
        del self.policies[policy_id]
        return True

    def apply_tag(self, entity: str, tag: str, value: Any = None) -> bool:
        if entity not in self.tags:
            self.tags[entity] = {}
        self.tags[entity][tag] = value
        return True

    def get_tags(self, entity: str) -> Dict[str, Any]:
        return self.tags.get(entity, {})

    def remove_tag(self, entity: str, tag: str) -> bool:
        if entity in self.tags and tag in self.tags[entity]:
            del self.tags[entity][tag]
            return True
        return False

    def set_classification(self, entity: str, classification: str) -> bool:
        self.classifications[entity] = classification
        return True

    def get_classification(self, entity: str) -> Optional[str]:
        return self.classifications.get(entity)

    def add_glossary_term(self, term: str, definition: str, metadata: Optional[Dict] = None) -> bool:
        self.glossary[term] = {'definition': definition, 'metadata': metadata or {}}
        return True

    def get_glossary_term(self, term: str) -> Optional[Dict[str, Any]]:
        return self.glossary.get(term)

    def export_governance(self) -> str:
        data = {'policies': self.policies, 'tags': self.tags, 'classifications': self.classifications, 'glossary': self.glossary}
        return json.dumps(data, indent=2)
