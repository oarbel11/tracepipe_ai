"""Webhook handler for Git events."""
import json
from typing import Dict, Any


class WebhookHandler:
    """Handles incoming webhooks from Git providers."""

    def __init__(self):
        self.supported_events = ["push", "pull_request", "merge_request"]

    def handle_webhook(self, payload: Dict[str, Any],
                       provider: str = "github") -> Dict[str, Any]:
        """Process incoming webhook payload."""
        event_type = self._extract_event_type(payload, provider)

        if event_type not in self.supported_events:
            return {"status": "ignored", "reason": "unsupported_event"}

        changes = self._extract_changes(payload, provider)
        return {
            "status": "success",
            "event_type": event_type,
            "changes": changes
        }

    def _extract_event_type(self, payload: Dict, provider: str) -> str:
        """Extract event type from payload."""
        if provider == "github":
            if "pull_request" in payload:
                return "pull_request"
            elif "commits" in payload:
                return "push"
        elif provider == "gitlab":
            return payload.get("object_kind", "unknown")
        return "unknown"

    def _extract_changes(self, payload: Dict, provider: str) -> Dict:
        """Extract file changes from payload."""
        files = []
        if provider == "github" and "pull_request" in payload:
            files = payload.get("pull_request", {}).get("changed_files", [])
        elif "commits" in payload:
            for commit in payload.get("commits", []):
                files.extend(commit.get("added", []))
                files.extend(commit.get("modified", []))

        return {
            "files": files,
            "repository": payload.get("repository", {})
        }
