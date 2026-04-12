"""Approval workflow management."""
from typing import Dict, List, Any
from datetime import datetime


class ApprovalWorkflow:
    """Manages approval workflows for pipeline changes."""
    
    def __init__(self, approvers: List[str] = None):
        self.approvers = approvers or []
        self.pending_approvals = []
    
    def request_approval(self, change_id: str, 
                        metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Request approval for a change."""
        approval = {
            "change_id": change_id,
            "status": "pending",
            "requested_at": datetime.utcnow().isoformat(),
            "approvers": self.approvers,
            "metadata": metadata
        }
        self.pending_approvals.append(approval)
        return approval
    
    def approve(self, change_id: str, approver: str) -> Dict[str, Any]:
        """Approve a change."""
        for approval in self.pending_approvals:
            if approval["change_id"] == change_id:
                approval["status"] = "approved"
                approval["approved_by"] = approver
                approval["approved_at"] = datetime.utcnow().isoformat()
                return approval
        return {"error": "Change not found"}
    
    def reject(self, change_id: str, approver: str, 
               reason: str = "") -> Dict[str, Any]:
        """Reject a change."""
        for approval in self.pending_approvals:
            if approval["change_id"] == change_id:
                approval["status"] = "rejected"
                approval["rejected_by"] = approver
                approval["reason"] = reason
                return approval
        return {"error": "Change not found"}
