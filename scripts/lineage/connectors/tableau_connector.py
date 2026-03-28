"""Tableau connector for lineage extraction."""

from typing import List, Dict, Any
from .base_connector import BaseConnector


class TableauConnector(BaseConnector):
    """Extracts lineage from Tableau workbooks."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connector_type = "tableau"
        self.server_url = config.get("server_url", "")

    def extract_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Tableau workbooks."""
        workbooks = self.get_entities()
        lineage = []
        for workbook in workbooks:
            lineage.append({
                "entity_id": f"tableau://{workbook}",
                "entity_type": "tableau_workbook",
                "metadata": {
                    "workbook": workbook,
                    "server_url": self.server_url
                }
            })
        return lineage

    def get_entities(self) -> List[str]:
        """Get list of Tableau workbooks."""
        return self.config.get("workbooks", [])
