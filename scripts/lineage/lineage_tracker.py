"""Main lineage tracking coordinator."""

from typing import Dict, List, Any, Optional
from .unmanaged_capture import UnmanagedLineageCapture
from .udf_mapper import UDFColumnMapper


class LineageTracker:
    """Coordinates lineage tracking across unmanaged files and UDFs."""

    def __init__(self):
        self.unmanaged_capture = UnmanagedLineageCapture()
        self.udf_mapper = UDFColumnMapper()
        self.lineage_graph: Dict[str, List[Dict[str, Any]]] = {}

    def track_operation(self, code: str, context: Dict[str, Any]
                        ) -> Optional[Dict[str, Any]]:
        """Track a data operation."""
        operation = self.unmanaged_capture.capture_write_operation(
            code, context
        )
        
        if operation:
            target = operation.get('target_path', '')
            if target not in self.lineage_graph:
                self.lineage_graph[target] = []
            self.lineage_graph[target].append(operation)
        
        return operation

    def get_full_lineage(self, target: str) -> Dict[str, Any]:
        """Get complete lineage for a target."""
        operations = self.lineage_graph.get(target, [])
        
        return {
            'target': target,
            'operations': operations,
            'source_count': len(operations)
        }

    def export_lineage(self) -> Dict[str, Any]:
        """Export all tracked lineage."""
        return {
            'lineage_graph': self.lineage_graph,
            'udf_mappings': self.udf_mapper.udf_mappings,
            'total_operations': sum(
                len(ops) for ops in self.lineage_graph.values()
            )
        }

    def reset(self):
        """Reset all tracking."""
        self.unmanaged_capture.reset()
        self.udf_mapper.udf_mappings.clear()
        self.lineage_graph.clear()
