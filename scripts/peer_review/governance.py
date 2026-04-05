class GovernancePolicy:
    def __init__(self, name, tags=None, rules=None, severity="medium", owner=None):
        self.name = name
        self.tags = tags or []
        self.rules = rules or []
        self.severity = severity
        self.owner = owner

    def applies_to_asset(self, asset_metadata):
        asset_tags = asset_metadata.get("tags", [])
        if not self.tags:
            return True
        return any(tag in asset_tags for tag in self.tags)

    def to_dict(self):
        return {
            "name": self.name,
            "tags": self.tags,
            "rules": self.rules,
            "severity": self.severity,
            "owner": self.owner
        }


class PolicyEngine:
    def __init__(self):
        self.policies = []

    def add_policy(self, policy):
        self.policies.append(policy)

    def get_applicable_policies(self, asset_metadata):
        return [p for p in self.policies if p.applies_to_asset(asset_metadata)]

    def load_default_policies(self):
        self.add_policy(GovernancePolicy(
            name="PII Data Protection",
            tags=["PII"],
            rules=["Encryption at rest required", "Masking for non-prod"],
            severity="high"
        ))
        self.add_policy(GovernancePolicy(
            name="Financial Data Compliance",
            tags=["financial", "revenue"],
            rules=["Audit logging required", "SOX compliance"],
            severity="high"
        ))
        self.add_policy(GovernancePolicy(
            name="General Data Quality",
            tags=[],
            rules=["Freshness < 24h", "Completeness > 95%"],
            severity="medium"
        ))
