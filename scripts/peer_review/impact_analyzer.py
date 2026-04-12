from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ImpactAnalyzer:
    def __init__(self, metadata_store: Optional[Any] = None):
        self.metadata_store = metadata_store
        logger.info("ImpactAnalyzer initialized")

    def analyze_changes(self, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze impact of changes to data pipelines."""
        if not changes:
            return {
                "risk_level": "low",
                "affected_pipelines": [],
                "affected_datasets": [],
                "recommendations": []
            }

        affected_pipelines = []
        affected_datasets = []
        risk_level = "low"

        for change in changes:
            file_path = change.get("file_path", "")
            change_type = change.get("change_type", "modified")

            if "pipeline" in file_path or file_path.endswith(".sql"):
                affected_pipelines.append(file_path)

            if "schema" in file_path or "model" in file_path:
                affected_datasets.append(file_path)
                if change_type == "deleted":
                    risk_level = "high"
                elif risk_level == "low":
                    risk_level = "medium"

        return {
            "risk_level": risk_level,
            "affected_pipelines": affected_pipelines,
            "affected_datasets": affected_datasets,
            "recommendations": self._generate_recommendations(risk_level)
        }

    def _generate_recommendations(self, risk_level: str) -> List[str]:
        if risk_level == "high":
            return ["Request senior review", "Run full test suite"]
        elif risk_level == "medium":
            return ["Run integration tests"]
        return ["Proceed with standard review"]


class InteractiveImpactAnalyzer(ImpactAnalyzer):
    def __init__(self, metadata_store: Optional[Any] = None):
        super().__init__(metadata_store)
        logger.info("InteractiveImpactAnalyzer initialized")

    def analyze_interactive(self, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Interactive analysis with user feedback."""
        base_analysis = self.analyze_changes(changes)
        base_analysis["interactive"] = True
        return base_analysis
