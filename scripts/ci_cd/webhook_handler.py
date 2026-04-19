"""Webhook handler for CI/CD integrations."""
import json
from typing import Dict, Any, Optional
from scripts.ci_cd.policy_engine import PolicyEngine
from scripts.ci_cd.feedback_generator import FeedbackGenerator


class CICDWebhookHandler:
    """Handles webhook events from CI/CD platforms."""

    def __init__(self, policy_config: Optional[str] = None):
        self.policy_engine = PolicyEngine(policy_config)
        self.feedback_generator = FeedbackGenerator()

    def handle_github_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GitHub webhook event."""
        event_type = payload.get('action')
        if event_type not in ['opened', 'synchronize', 'reopened']:
            return {"status": "ignored", "reason": "Not a PR event"}

        pr_data = self._extract_github_pr_data(payload)
        return self._process_pr(pr_data)

    def handle_gitlab_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GitLab webhook event."""
        event_type = payload.get('object_kind')
        if event_type != 'merge_request':
            return {"status": "ignored", "reason": "Not a MR event"}

        pr_data = self._extract_gitlab_pr_data(payload)
        return self._process_pr(pr_data)

    def _extract_github_pr_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Extract PR data from GitHub payload."""
        pr = payload.get('pull_request', {})
        return {
            "id": pr.get('number'),
            "title": pr.get('title', ''),
            "diff": pr.get('body', ''),
            "files": [f.get('filename', '') for f in payload.get('files', [])],
            "platform": "github"
        }

    def _extract_gitlab_pr_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Extract MR data from GitLab payload."""
        mr = payload.get('object_attributes', {})
        return {
            "id": mr.get('iid'),
            "title": mr.get('title', ''),
            "diff": mr.get('description', ''),
            "files": [],
            "platform": "gitlab"
        }

    def _process_pr(self, pr_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process PR through policy engine and generate feedback."""
        violations = self.policy_engine.evaluate(pr_data)
        feedback = self.feedback_generator.generate(violations)

        status = "failure" if any(v['severity'] == 'critical' for v in violations) else "success"

        return {
            "status": status,
            "pr_id": pr_data.get('id'),
            "violations": violations,
            "feedback": feedback,
            "should_block": status == "failure"
        }
