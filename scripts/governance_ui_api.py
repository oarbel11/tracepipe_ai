from typing import Dict, List, Optional
import json
from scripts.lineage_ui_manager import LineageUIManager
from scripts.peer_review.governance_policy import GovernancePolicy

class GovernanceUIAPI:
    def __init__(self, lineage_manager: LineageUIManager):
        self.lineage_manager = lineage_manager
        self.policies: Dict[str, GovernancePolicy] = {}

    def register_policy(self, policy: GovernancePolicy):
        self.policies[policy.policy_id] = policy

    def apply_policy_to_asset(self, asset_id: str, policy_id: str) -> Dict:
        if policy_id not in self.policies:
            return {"success": False, "error": "Policy not found"}
        policy = self.policies[policy_id]
        for rule_key, rule_value in policy.rules.items():
            if rule_key == "classification":
                self.lineage_manager.apply_classification(
                    asset_id, rule_value, f"Applied by policy {policy.name}"
                )
            elif rule_key == "masking":
                self.lineage_manager.conn.execute(
                    "UPDATE asset_metadata SET masking_policy = ? WHERE asset_id = ?",
                    [rule_value, asset_id]
                )
            elif rule_key == "tag":
                self._add_tag(asset_id, rule_value)
        return {"success": True, "policy": policy.name, "asset": asset_id}

    def _add_tag(self, asset_id: str, tag: str):
        existing = self.lineage_manager.conn.execute(
            "SELECT tags FROM asset_metadata WHERE asset_id = ?", [asset_id]
        ).fetchone()
        tags = json.loads(existing[0]) if existing and existing[0] else []
        if tag not in tags:
            tags.append(tag)
        if existing:
            self.lineage_manager.conn.execute(
                "UPDATE asset_metadata SET tags = ? WHERE asset_id = ?",
                [json.dumps(tags), asset_id]
            )
        else:
            self.lineage_manager.conn.execute(
                "INSERT INTO asset_metadata (asset_id, tags) VALUES (?, ?)",
                [asset_id, json.dumps(tags)]
            )

    def get_asset_governance_info(self, asset_id: str) -> Dict:
        metadata = self.lineage_manager.conn.execute(
            "SELECT classifications, business_terms, tags, masking_policy FROM asset_metadata WHERE asset_id = ?",
            [asset_id]
        ).fetchone()
        if not metadata:
            return {"asset_id": asset_id, "classifications": [], "business_terms": [], "tags": [], "masking_policy": None}
        return {
            "asset_id": asset_id,
            "classifications": json.loads(metadata[0]) if metadata[0] else [],
            "business_terms": json.loads(metadata[1]) if metadata[1] else [],
            "tags": json.loads(metadata[2]) if metadata[2] else [],
            "masking_policy": metadata[3]
        }

    def bulk_apply_policy(self, asset_ids: List[str], policy_id: str) -> Dict:
        results = []
        for asset_id in asset_ids:
            result = self.apply_policy_to_asset(asset_id, policy_id)
            results.append(result)
        success_count = sum(1 for r in results if r.get("success"))
        return {"total": len(asset_ids), "success": success_count, "failed": len(asset_ids) - success_count}

    def detect_lineage_issues(self) -> List[Dict]:
        issues = []
        edges = self.lineage_manager.conn.execute(
            "SELECT source_asset, target_asset, COUNT(*) as cnt FROM lineage_edges WHERE is_active GROUP BY source_asset, target_asset HAVING cnt > 1"
        ).fetchall()
        for source, target, count in edges:
            issues.append({"type": "duplicate_edge", "source": source, "target": target, "count": count})
        orphaned = [n for n in self.lineage_manager.graph.nodes() if self.lineage_manager.graph.degree(n) == 0]
        for node in orphaned:
            issues.append({"type": "orphaned_node", "asset": node})
        return issues
