"""CLI script to run policy checks in CI/CD pipeline."""
import sys
import json
import os
from scripts.cicd.policy_enforcer import PolicyEnforcer
from scripts.cicd.webhook_handler import WebhookHandler


def main():
    """Main entry point for policy check."""
    event_path = os.getenv("GITHUB_EVENT_PATH", "")
    if not event_path or not os.path.exists(event_path):
        print("No event data found")
        sys.exit(1)

    with open(event_path, "r") as f:
        payload = json.load(f)

    handler = WebhookHandler()
    enforcer = PolicyEnforcer()

    webhook_result = handler.handle_webhook(payload, "github")
    if webhook_result["status"] != "success":
        print(f"Webhook processing failed: {webhook_result}")
        sys.exit(1)

    changes = webhook_result["changes"]
    result = enforcer.enforce_policies(changes)

    print(json.dumps(result, indent=2))

    if not result["passed"]:
        print("\nPolicy violations detected!")
        for violation in result["violations"]:
            print(f"  - {violation}")
        sys.exit(1)

    if result["requires_approval"]:
        print("\nManual approval required due to high impact")
        sys.exit(1)

    print("\nAll policy checks passed!")
    sys.exit(0)


if __name__ == "__main__":
    main()
