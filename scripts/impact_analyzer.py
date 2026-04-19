"""Analyzes impact of schema changes."""
from typing import Dict, List, Any


class ImpactAnalyzer:
    """Analyzes impact of schema and policy changes."""

    def __init__(self, lineage_manager):
        self.lineage_manager = lineage_manager
        self.schema_registry = {}

    def register_schema(self, entity: str, schema: Dict[str, str]):
        """Register entity schema."""
        self.schema_registry[entity] = schema

    def analyze_column_removal(self, entity: str, column: str) -> Dict:
        """Analyze impact of removing a column."""
        if entity not in self.schema_registry:
            return {"error": "Entity not found"}

        downstream = self.lineage_manager.get_downstream(entity)
        impacted = []

        for downstream_entity in downstream:
            if downstream_entity in self.schema_registry:
                if column in self.schema_registry[downstream_entity]:
                    impacted.append({
                        "entity": downstream_entity,
                        "column": column,
                        "impact": "column_used"
                    })

        return {
            "entity": entity,
            "column": column,
            "action": "remove",
            "impacted_entities": impacted,
            "severity": "high" if impacted else "low"
        }

    def analyze_column_rename(self, entity: str, old_name: str,
                             new_name: str) -> Dict:
        """Analyze impact of renaming a column."""
        removal_impact = self.analyze_column_removal(entity, old_name)
        return {
            "entity": entity,
            "old_column": old_name,
            "new_column": new_name,
            "action": "rename",
            "impacted_entities": removal_impact.get("impacted_entities", []),
            "severity": removal_impact.get("severity", "low"),
            "migration_required": len(removal_impact.get("impacted_entities", [])) > 0
        }

    def analyze_type_change(self, entity: str, column: str,
                           old_type: str, new_type: str) -> Dict:
        """Analyze impact of changing column type."""
        downstream = self.lineage_manager.get_downstream(entity)
        impacted = []

        for downstream_entity in downstream:
            if downstream_entity in self.schema_registry:
                if column in self.schema_registry[downstream_entity]:
                    impacted.append({
                        "entity": downstream_entity,
                        "column": column,
                        "impact": "type_mismatch_possible"
                    })

        return {
            "entity": entity,
            "column": column,
            "old_type": old_type,
            "new_type": new_type,
            "impacted_entities": impacted,
            "severity": "high" if impacted else "medium"
        }
