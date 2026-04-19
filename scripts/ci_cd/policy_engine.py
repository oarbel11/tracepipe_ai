"""Policy engine for evaluating governance rules."""
import json
import re
from typing import Dict, List, Any


class PolicyEngine:
    """Evaluates PRs against governance policies."""

    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.policies = self._load_policies()

    def _load_policies(self) -> Dict[str, Any]:
        """Load policy configuration."""
        if self.config_path:
            try:
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            except FileNotFoundError:
                pass
        return self._default_policies()

    def _default_policies(self) -> Dict[str, Any]:
        """Default governance policies."""
        return {
            "pii_detection": {"enabled": True, "severity": "critical"},
            "schema_changes": {"enabled": True, "severity": "high"},
            "performance_impact": {"enabled": True, "severity": "medium"}
        }

    def evaluate(self, pr_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate PR against policies."""
        violations = []
        diff = pr_data.get('diff', '')
        files = pr_data.get('files', [])

        if self.policies.get('pii_detection', {}).get('enabled'):
            violations.extend(self._check_pii(diff, files))

        if self.policies.get('schema_changes', {}).get('enabled'):
            violations.extend(self._check_schema(diff, files))

        if self.policies.get('performance_impact', {}).get('enabled'):
            violations.extend(self._check_performance(diff, files))

        return violations

    def _check_pii(self, diff: str, files: List[str]) -> List[Dict[str, Any]]:
        """Check for PII exposure."""
        pii_patterns = [r'ssn', r'social.security', r'credit.card', r'email']
        violations = []
        for pattern in pii_patterns:
            if re.search(pattern, diff, re.IGNORECASE):
                violations.append({
                    "type": "pii_exposure",
                    "severity": "critical",
                    "message": f"Potential PII exposure detected: {pattern}"
                })
        return violations

    def _check_schema(self, diff: str, files: List[str]) -> List[Dict[str, Any]]:
        """Check for breaking schema changes."""
        violations = []
        schema_files = [f for f in files if 'schema' in f.lower() or '.sql' in f]
        if schema_files and 'DROP' in diff.upper():
            violations.append({
                "type": "schema_breaking",
                "severity": "high",
                "message": "Breaking schema change detected (DROP statement)"
            })
        return violations

    def _check_performance(self, diff: str, files: List[str]) -> List[Dict[str, Any]]:
        """Check for performance issues."""
        violations = []
        if re.search(r'SELECT \*', diff, re.IGNORECASE):
            violations.append({
                "type": "performance_issue",
                "severity": "medium",
                "message": "SELECT * detected - consider explicit columns"
            })
        return violations
