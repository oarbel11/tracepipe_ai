"""Power BI connector for lineage extraction."""

from typing import List, Dict, Any
from .base_connector import BaseConnector


class PowerBIConnector(BaseConnector):
    """Extracts lineage from Power BI reports."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connector_type = "powerbi"
        self.workspace_id = config.get("workspace_id", "")

    def extract_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Power BI reports."""
        reports = self.get_entities()
        lineage = []
        for report in reports:
            lineage.append({
                "entity_id": f"powerbi://{self.workspace_id}/{report}",
                "entity_type": "powerbi_report",
                "metadata": {
                    "report": report,
                    "workspace_id": self.workspace_id
                }
            })
        return lineage

    def get_entities(self) -> List[str]:
        """Get list of Power BI reports."""
        return self.config.get("reports", [])
