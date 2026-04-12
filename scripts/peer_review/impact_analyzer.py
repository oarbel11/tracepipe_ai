"""Impact analyzer for data pipeline changes."""
import json
from typing import Dict, List, Any


class ImpactAnalyzer:
    """Analyzes impact of data pipeline changes."""

    def __init__(self):
        self.impacts = []

    def analyze_changes(self, changes: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pipeline changes and return impact report."""
        impacted_pipelines = self._identify_impacted_pipelines(changes)
        data_quality_impact = self._check_data_quality_impact(changes)
        downstream_impact = self._analyze_downstream_impact(changes)

        return {
            "impacted_pipelines": impacted_pipelines,
            "data_quality_impact": data_quality_impact,
            "downstream_impact": downstream_impact,
            "severity": self._calculate_severity(impacted_pipelines,
                                                  data_quality_impact)
        }

    def _identify_impacted_pipelines(self, changes: Dict) -> List[str]:
        """Identify which pipelines are impacted."""
        pipelines = []
        if "files" in changes:
            for file in changes["files"]:
                if "pipeline" in file.lower() or ".sql" in file.lower():
                    pipelines.append(file)
        return pipelines

    def _check_data_quality_impact(self, changes: Dict) -> Dict[str, Any]:
        """Check if changes affect data quality rules."""
        return {
            "rules_affected": [],
            "new_rules_needed": False
        }

    def _analyze_downstream_impact(self, changes: Dict) -> Dict[str, Any]:
        """Analyze downstream dependencies."""
        return {
            "affected_tables": [],
            "affected_dashboards": []
        }

    def _calculate_severity(self, pipelines: List, quality: Dict) -> str:
        """Calculate severity of impact."""
        if len(pipelines) > 5:
            return "high"
        elif len(pipelines) > 2:
            return "medium"
        return "low"
