from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field
from scripts.peer_review.governance_policy import GovernancePolicy

@dataclass
class EnforcementResult:
    asset_id: str
    policy_id: str
    action: str
    tags_applied: List[str] = field(default_factory=list)
    violations: List[str] = field(default_factory=list)
    access_level: Optional[str] = None

class PolicyEnforcer:
    def __init__(self, policies: List[GovernancePolicy]):
        self.policies = policies
        self.enforcement_cache: Dict[str, List[str]] = {}

    def propagate_tags(self, lineage: Dict[str, List[str]], 
                      asset_tags: Dict[str, List[str]]) -> Dict[str, Set[str]]:
        propagated = {k: set(v) for k, v in asset_tags.items()}
        changed = True
        max_iterations = 100
        iteration = 0
        
        while changed and iteration < max_iterations:
            changed = False
            iteration += 1
            for parent, children in lineage.items():
                parent_tags = propagated.get(parent, set())
                for child in children:
                    child_tags = propagated.get(child, set())
                    new_tags = parent_tags - child_tags
                    if new_tags:
                        propagated[child] = child_tags | new_tags
                        changed = True
        
        return {k: list(v) for k, v in propagated.items()}

    def enforce_policies(self, lineage: Dict[str, List[str]], 
                        asset_tags: Dict[str, List[str]]) -> List[EnforcementResult]:
        propagated_tags = self.propagate_tags(lineage, asset_tags)
        results = []
        
        all_assets = set(lineage.keys()) | set(
            child for children in lineage.values() for child in children
        )
        
        for asset_id in all_assets:
            tags = propagated_tags.get(asset_id, [])
            for policy in self.policies:
                if policy.matches_asset(tags, asset_id):
                    result = self._apply_policy(asset_id, policy, tags)
                    results.append(result)
        
        return results

    def _apply_policy(self, asset_id: str, policy: GovernancePolicy, 
                     tags: List[str]) -> EnforcementResult:
        action = policy.rules.get('action', 'monitor')
        access_level = policy.rules.get('access_level')
        violations = []
        
        if action == 'restrict' and 'PII' in tags:
            violations.append(f"Asset {asset_id} contains PII")
        
        quality_threshold = policy.rules.get('quality_threshold')
        if quality_threshold and 'low_quality' in tags:
            violations.append(f"Asset {asset_id} below quality threshold")
        
        return EnforcementResult(
            asset_id=asset_id,
            policy_id=policy.policy_id,
            action=action,
            tags_applied=tags,
            violations=violations,
            access_level=access_level
        )
