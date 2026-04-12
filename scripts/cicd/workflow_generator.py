import os
import argparse

GITHUB_WORKFLOW = """name: Tracepipe AI Policy Check

on:
  pull_request:
    types: [opened, synchronize, reopened]
    paths:
      - '**/*.sql'
      - '**/etl/**'

jobs:
  policy-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Tracepipe AI
        run: |
          pip install -r requirements.txt
      
      - name: Run Policy Enforcement
        run: |
          python -m scripts.cicd.policy_enforcer_cli \
            --files $(git diff --name-only origin/${{ github.base_ref }}...HEAD) \
            --repo ${{ github.repository }} \
            --output policy-results.json
      
      - name: Comment PR
        if: always()
        uses: actions/github-script@v6
        with:
          script: |
            const fs = require('fs');
            const results = JSON.parse(fs.readFileSync('policy-results.json', 'utf8'));
            const body = results.comment || 'Policy check completed';
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: body
            });
"""

GITLAB_WORKFLOW = """tracepipe_policy_check:
  stage: test
  image: python:3.10
  script:
    - pip install -r requirements.txt
    - |
      python -m scripts.cicd.policy_enforcer_cli \
        --files $(git diff --name-only $CI_MERGE_REQUEST_TARGET_BRANCH_SHA...HEAD) \
        --repo $CI_PROJECT_PATH \
        --output policy-results.json
  only:
    - merge_requests
  artifacts:
    reports:
      junit: policy-results.json
"""

def generate_workflow(platform: str, output_path: str) -> None:
    template = GITHUB_WORKFLOW if platform == 'github' else GITLAB_WORKFLOW
    
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(template)
    
    print(f"Generated {platform} workflow at: {output_path}")
    print(f"\nNext steps:")
    print(f"1. Commit this file to your repository")
    print(f"2. Configure webhook (if using webhook server)")
    print(f"3. Create a PR to test the integration")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate CI/CD workflow for Tracepipe AI')
    parser.add_argument('--platform', choices=['github', 'gitlab'], required=True)
    parser.add_argument('--output', required=True, help='Output file path')
    args = parser.parse_args()
    
    generate_workflow(args.platform, args.output)
