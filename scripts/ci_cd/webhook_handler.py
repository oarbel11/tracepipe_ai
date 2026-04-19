from typing import Dict, List, Optional
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from scripts.ci_cd.policy_engine import PolicyEngine
from scripts.ci_cd.feedback_generator import FeedbackGenerator

class WebhookRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        event = json.loads(body.decode('utf-8'))
        
        result = self.server.handler.process_event(event)
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(result).encode('utf-8'))

class CICDWebhookHandler:
    def __init__(self, config_path: str = 'config/policies.yml'):
        self.policy_engine = PolicyEngine(config_path)
        self.feedback_generator = FeedbackGenerator()
    
    def process_event(self, event: Dict) -> Dict:
        event_type = event.get('action') or event.get('object_kind')
        
        if event_type in ['opened', 'synchronize', 'merge_request']:
            return self._handle_pr_event(event)
        return {'status': 'ignored', 'message': 'Event type not handled'}
    
    def _handle_pr_event(self, event: Dict) -> Dict:
        pr_data = self._extract_pr_data(event)
        violations = self.policy_engine.evaluate(pr_data)
        
        feedback = self.feedback_generator.generate(
            violations=violations,
            pr_data=pr_data
        )
        
        status = 'failure' if any(v['severity'] == 'critical' for v in violations) else 'success'
        
        return {
            'status': status,
            'violations': violations,
            'feedback': feedback,
            'can_merge': status == 'success'
        }
    
    def _extract_pr_data(self, event: Dict) -> Dict:
        if 'pull_request' in event:
            pr = event['pull_request']
            return {
                'id': pr.get('number'),
                'title': pr.get('title'),
                'files': pr.get('changed_files', []),
                'diff': pr.get('diff_url'),
                'author': pr.get('user', {}).get('login')
            }
        elif 'object_attributes' in event:
            mr = event['object_attributes']
            return {
                'id': mr.get('iid'),
                'title': mr.get('title'),
                'files': event.get('changes', []),
                'diff': mr.get('diff_url'),
                'author': mr.get('author', {}).get('username')
            }
        return {}
    
    def start_server(self, host: str = '0.0.0.0', port: int = 8080):
        server = HTTPServer((host, port), WebhookRequestHandler)
        server.handler = self
        print(f"CI/CD Webhook Handler listening on {host}:{port}")
        server.serve_forever()
