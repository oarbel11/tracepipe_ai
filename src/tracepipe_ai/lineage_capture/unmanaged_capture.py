"""Capture lineage for unmanaged files and tables."""

import re
from typing import Dict, List, Optional, Any


class UnmanagedCapture:
    """Captures lineage for unmanaged files written to cloud storage."""

    def __init__(self):
        self.operations: List[Dict[str, Any]] = []

    def capture_write_operation(
        self,
        path: str,
        source_tables: List[str],
        columns: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Capture a write operation to an unmanaged location."""
        operation = {
            'path': path,
            'source_tables': source_tables,
            'columns': columns or [],
            'metadata': metadata or {},
            'operation_type': 'write'
        }
        self.operations.append(operation)
        return operation

    def capture_read_operation(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Capture a read operation from an unmanaged location."""
        operation = {
            'path': path,
            'columns': columns or [],
            'metadata': metadata or {},
            'operation_type': 'read'
        }
        self.operations.append(operation)
        return operation

    def get_lineage(self, path: str) -> List[Dict[str, Any]]:
        """Get lineage information for a specific path."""
        pattern = re.compile(re.escape(path))
        return [
            op for op in self.operations
            if pattern.search(op['path'])
        ]

    def clear(self):
        """Clear all captured operations."""
        self.operations.clear()
