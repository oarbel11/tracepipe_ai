from typing import Dict, Any, Optional
from .workflow_engine import WorkflowEngine

class PeerReviewSystem:
    def __init__(self, workflow_config: Optional[Dict[str, Any]] = None):
        self.workflow_engine = None
        if workflow_config:
            self.workflow_engine = WorkflowEngine(workflow_config)

    def review_change(self, change_data: Dict[str, Any]) -> Dict[str, Any]:
        impact_analysis = self._analyze_impact(change_data)
        
        if self.workflow_engine:
            workflow_result = self.workflow_engine.process_change(change_data, impact_analysis)
            return {"impact_analysis": impact_analysis, "workflow_result": workflow_result}
        
        return {"impact_analysis": impact_analysis}

    def _analyze_impact(self, change_data: Dict[str, Any]) -> Dict[str, Any]:
        operation = change_data.get("operation", "unknown")
        rows_affected = change_data.get("rows_affected", 0)
        
        severity = "low"
        if rows_affected > 10000:
            severity = "critical"
        elif rows_affected > 1000:
            severity = "high"
        elif rows_affected > 100:
            severity = "medium"
        
        return {
            "operation": operation,
            "rows_affected": rows_affected,
            "severity": severity,
            "risk_level": self._calculate_risk(operation, rows_affected)
        }

    def _calculate_risk(self, operation: str, rows_affected: int) -> str:
        if operation == "DROP" or rows_affected > 10000:
            return "high"
        elif operation in ["DELETE", "UPDATE"] and rows_affected > 1000:
            return "medium"
        return "low"
