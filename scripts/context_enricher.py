import json
from typing import Dict, Any, List

class ContextEnricher:
    """Enriches workspace objects with additional context and metadata."""
    
    def __init__(self):
        self.enriched_objects = {}
    
    def enrich_notebook(self, notebook: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich notebook with additional context."""
        enriched = notebook.copy()
        enriched["enriched_metadata"] = {
            "object_type": "notebook",
            "language": notebook.get("language", "unknown"),
            "path": notebook.get("path", ""),
            "has_schedule": False,
            "last_modified": "2024-01-01T00:00:00Z"
        }
        return enriched
    
    def enrich_dashboard(self, dashboard: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich dashboard with additional context."""
        enriched = dashboard.copy()
        enriched["enriched_metadata"] = {
            "object_type": "dashboard",
            "query_count": 0,
            "last_viewed": "2024-01-01T00:00:00Z"
        }
        return enriched
    
    def enrich_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich job with additional context."""
        enriched = job.copy()
        enriched["enriched_metadata"] = {
            "object_type": "job",
            "schedule": "daily",
            "last_run": "2024-01-01T00:00:00Z",
            "status": "success"
        }
        return enriched
    
    def enrich_objects(self, objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich a list of objects based on their type."""
        enriched_list = []
        for obj in objects:
            obj_type = obj.get("type")
            if obj_type == "notebook":
                enriched_list.append(self.enrich_notebook(obj))
            elif obj_type == "dashboard":
                enriched_list.append(self.enrich_dashboard(obj))
            elif obj_type == "job":
                enriched_list.append(self.enrich_job(obj))
            else:
                enriched_list.append(obj)
        return enriched_list
