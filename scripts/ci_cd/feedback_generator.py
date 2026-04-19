"""Generates actionable feedback from policy violations."""
from typing import List, Dict, Any


class FeedbackGenerator:
    """Generates actionable AI feedback for PR reviews."""

    def generate(self, violations: List[Dict[str, Any]]) -> str:
        """Generate feedback message from violations."""
        if not violations:
            return "✅ All policy checks passed. No violations detected."

        feedback_lines = ["## 🤖 AI Policy Review\n"]
        critical = [v for v in violations if v['severity'] == 'critical']
        high = [v for v in violations if v['severity'] == 'high']
        medium = [v for v in violations if v['severity'] == 'medium']

        if critical:
            feedback_lines.append("### ❌ Critical Issues (Blocking)")
            for v in critical:
                feedback_lines.append(f"- **{v['type']}**: {v['message']}")
                feedback_lines.append(f"  *Action*: {self._get_action(v['type'])}")
            feedback_lines.append("")

        if high:
            feedback_lines.append("### ⚠️ High Priority Issues")
            for v in high:
                feedback_lines.append(f"- **{v['type']}**: {v['message']}")
                feedback_lines.append(f"  *Action*: {self._get_action(v['type'])}")
            feedback_lines.append("")

        if medium:
            feedback_lines.append("### ℹ️ Medium Priority Issues")
            for v in medium:
                feedback_lines.append(f"- **{v['type']}**: {v['message']}")
                feedback_lines.append(f"  *Action*: {self._get_action(v['type'])}")

        return "\n".join(feedback_lines)

    def _get_action(self, violation_type: str) -> str:
        """Get actionable recommendation for violation type."""
        actions = {
            "pii_exposure": "Remove PII or add proper masking/encryption",
            "schema_breaking": "Use ALTER instead of DROP or add migration",
            "performance_issue": "Specify explicit columns instead of SELECT *"
        }
        return actions.get(violation_type, "Review and address this issue")
