"""CI/CD plugin for orchestrating policy enforcement."""
import json
from typing import Dict, Any, Optional
from scripts.ci_cd.webhook_handler import CICDWebhookHandler


class CICDPlugin:
    """Orchestrates CI/CD integration for policy enforcement."""

    def __init__(self, config_path: Optional[str] = None):
        self.webhook_handler = CICDWebhookHandler(config_path)

    def process_webhook(self, platform: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Process webhook from CI/CD platform."""
        if platform == "github":
            return self.webhook_handler.handle_github_webhook(payload)
        elif platform == "gitlab":
            return self.webhook_handler.handle_gitlab_webhook(payload)
        else:
            return {"status": "error", "message": f"Unsupported platform: {platform}"}

    def post_status(self, platform: str, result: Dict[str, Any]) -> bool:
        """Post status check to CI/CD platform."""
        if result['status'] == 'failure':
            return False
        return True

    def format_comment(self, result: Dict[str, Any]) -> str:
        """Format result as PR comment."""
        return result.get('feedback', '')
