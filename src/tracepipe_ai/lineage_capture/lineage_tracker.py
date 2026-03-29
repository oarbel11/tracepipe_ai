"""Unified lineage tracking across all components."""

from typing import Dict, List, Any, Optional
from .unmanaged_capture import UnmanagedCapture
from .udf_mapper import UDFMapper


class LineageTracker:
    """Unified lineage tracker for all operations."""

    def __init__(self):
        self.unmanaged_capture = UnmanagedCapture()
        self.udf_mapper = UDFMapper()
        self.operations: List[Dict[str, Any]] = []

    def track_operation(
        self,
        operation_type: str,
        details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Track a lineage operation."""
        operation = {
            'operation_type': operation_type,
            'details': details
        }
        self.operations.append(operation)

        if operation_type == 'unmanaged_write':
            self.unmanaged_capture.capture_write_operation(
                path=details.get('path', ''),
                source_tables=details.get('source_tables', []),
                columns=details.get('columns'),
                metadata=details.get('metadata')
            )
        elif operation_type == 'unmanaged_read':
            self.unmanaged_capture.capture_read_operation(
                path=details.get('path', ''),
                columns=details.get('columns'),
                metadata=details.get('metadata')
            )

        return operation

    def get_lineage_graph(self) -> Dict[str, Any]:
        """Generate a complete lineage graph."""
        return {
            'nodes': [],
            'edges': [],
            'operations': self.operations
        }

    def export_lineage(self, format: str = 'json') -> Dict[str, Any]:
        """Export lineage in specified format."""
        return {
            'format': format,
            'total_operations': len(self.operations),
            'operations': self.operations,
            'unmanaged_operations': len(self.unmanaged_capture.operations),
            'registered_udfs': len(self.udf_mapper.udf_registry)
        }
