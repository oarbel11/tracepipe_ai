import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Optional
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from scripts.cicd.policy_enforcer import PolicyEnforcer

class GitWebhookHandler(BaseHTTPRequestHandler):
    enforcer = PolicyEnforcer()

    def do_POST(self):
        if self.path != '/webhook/git':
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        event_type = self.headers.get('X-GitHub-Event', self.headers.get('X-Gitlab-Event', ''))

        try:
            payload = json.loads(body)
            result = self.handle_git_event(event_type, payload)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def handle_git_event(self, event_type: str, payload: Dict) -> Dict:
        if event_type in ['pull_request', 'merge_request']:
            return self.handle_pr_event(payload)
        elif event_type == 'push':
            return self.handle_push_event(payload)
        return {'status': 'ignored', 'event': event_type}

    def handle_pr_event(self, payload: Dict) -> Dict:
        action = payload.get('action')
        if action not in ['opened', 'synchronize', 'reopened']:
            return {'status': 'skipped', 'reason': f'action={action}'}

        pr_number = payload.get('pull_request', {}).get('number') or payload.get('object_attributes', {}).get('iid')
        repo = payload.get('repository', {}).get('full_name', 'unknown')
        changed_files = self.get_changed_files(payload)

        results = self.enforcer.enforce_policies(changed_files, repo)
        comment = self.enforcer.format_pr_comment(results)

        return {
            'status': 'analyzed',
            'pr': pr_number,
            'repo': repo,
            'results': results,
            'comment': comment
        }

    def handle_push_event(self, payload: Dict) -> Dict:
        repo = payload.get('repository', {}).get('full_name', 'unknown')
        commits = payload.get('commits', [])
        changed_files = [f for c in commits for f in c.get('added', []) + c.get('modified', [])]

        results = self.enforcer.enforce_policies(changed_files, repo)
        return {'status': 'push_analyzed', 'repo': repo, 'results': results}

    def get_changed_files(self, payload: Dict) -> list:
        pr = payload.get('pull_request') or payload.get('object_attributes', {})
        return pr.get('changed_files', [])

def start_server(port: int = 8080):
    server = HTTPServer(('0.0.0.0', port), GitWebhookHandler)
    print(f"Webhook server listening on port {port}")
    server.serve_forever()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()
    start_server(args.port)
