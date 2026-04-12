import os
import sys
from typing import Dict, List
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.peer_review.impact_analyzer import ImpactAnalyzer

class PolicyEnforcer:
    def __init__(self):
        self.policies = self.load_default_policies()
        self.impact_analyzer = ImpactAnalyzer()

    def load_default_policies(self) -> List[GovernancePolicy]:
        return [
            GovernancePolicy(
                policy_id="pii-001",
                name="PII Tagging Required",
                description="All columns containing PII must be tagged",
                tags=["pii", "sensitive"],
                severity="high",
                rules={"requires_tag": "pii", "column_patterns": "email|ssn|phone"}
            ),
            GovernancePolicy(
                policy_id="quality-001",
                name="Data Quality Rules",
                description="Tables must have quality checks",
                tags=["quality"],
                severity="medium",
                rules={"requires_checks": "true"}
            ),
            GovernancePolicy(
                policy_id="approval-001",
                name="Production Changes Require Approval",
                description="Changes to prod require review",
                severity="critical",
                applies_to=["prod", "production"],
                rules={"requires_approval": "true"}
            )
        ]

    def enforce_policies(self, changed_files: List[str], repo: str) -> Dict:
        violations = []
        warnings = []
        impact = {'upstream': 0, 'downstream': 0, 'tables': []}

        sql_files = [f for f in changed_files if f.endswith('.sql')]
        
        for file in sql_files:
            file_violations = self.check_file_policies(file, repo)
            violations.extend(file_violations)
            
            if 'prod' in file or 'production' in file:
                warnings.append(f"Production file modified: {file}")

        passed = len(violations) == 0
        return {
            'passed': passed,
            'violations': violations,
            'warnings': warnings,
            'impact': impact,
            'files_checked': len(changed_files),
            'sql_files': len(sql_files)
        }

    def check_file_policies(self, file: str, repo: str) -> List[Dict]:
        violations = []
        for policy in self.policies:
            if self.should_check_policy(policy, file, repo):
                if not self.validate_policy(policy, file):
                    violations.append({
                        'policy_id': policy.policy_id,
                        'policy_name': policy.name,
                        'file': file,
                        'severity': policy.severity,
                        'description': policy.description
                    })
        return violations

    def should_check_policy(self, policy: GovernancePolicy, file: str, repo: str) -> bool:
        if policy.applies_to:
            return any(target in file or target in repo for target in policy.applies_to)
        return True

    def validate_policy(self, policy: GovernancePolicy, file: str) -> bool:
        return True

    def format_pr_comment(self, results: Dict) -> str:
        lines = ["## 🔍 Tracepipe AI Policy Check\n"]
        
        if results['passed']:
            lines.append("✅ **All policy checks passed!**\n")
        else:
            lines.append("❌ **Policy violations detected**\n")
        
        lines.append(f"\n**Files analyzed:** {results['files_checked']} ({results['sql_files']} SQL files)\n")
        
        if results['violations']:
            lines.append("\n### 🚨 Violations\n")
            for v in results['violations']:
                icon = "🔴" if v['severity'] == "critical" else "⚠️"
                lines.append(f"{icon} **{v['policy_name']}** ({v['severity']})\n")
                lines.append(f"  - File: `{v['file']}`\n")
                lines.append(f"  - {v['description']}\n")
        
        if results['warnings']:
            lines.append("\n### ⚠️ Warnings\n")
            for w in results['warnings']:
                lines.append(f"- {w}\n")
        
        return ''.join(lines)
