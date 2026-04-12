"""CI/CD workflow template generator."""
from typing import Dict, Any


class WorkflowGenerator:
    """Generates CI/CD workflow configuration files."""

    def generate_github_actions(self, config: Dict[str, Any]) -> str:
        """Generate GitHub Actions workflow YAML."""
        workflow = f"""name: Tracepipe Policy Check
on:
  pull_request:
    branches: [{', '.join(config.get('branches', ['main']))}]
jobs:
  policy-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Policy Checks
        run: |
          python scripts/cicd/run_policy_check.py
        env:
          TRACEPIPE_API_KEY: ${{{{ secrets.TRACEPIPE_API_KEY }}}}
"""
        return workflow

    def generate_gitlab_ci(self, config: Dict[str, Any]) -> str:
        """Generate GitLab CI configuration YAML."""
        workflow = f"""policy-check:
  stage: test
  script:
    - python scripts/cicd/run_policy_check.py
  only:
    - merge_requests
  variables:
    TRACEPIPE_API_KEY: $TRACEPIPE_API_KEY
"""
        return workflow

    def generate_workflow(self, provider: str,
                          config: Dict[str, Any]) -> str:
        """Generate workflow for specified provider."""
        if provider == "github":
            return self.generate_github_actions(config)
        elif provider == "gitlab":
            return self.generate_gitlab_ci(config)
        else:
            raise ValueError(f"Unsupported provider: {provider}")
