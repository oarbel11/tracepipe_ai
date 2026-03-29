"""Lineage capture for unmanaged files and UDFs in Unity Catalog."""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime


class UnmanagedCapture:
    """Captures lineage for unmanaged files and cloud storage operations."""

    def __init__(self):
        self.operations = []

    def track_write(self, path: str, source_table: str, metadata: Dict) -> None:
        """Track write operation to unmanaged location."""
        operation = {
            "type": "write",
            "path": path,
            "source_table": source_table,
            "metadata": metadata,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.operations.append(operation)

    def track_read(self, path: str, target_table: str, metadata: Dict) -> None:
        """Track read operation from unmanaged location."""
        operation = {
            "type": "read",
            "path": path,
            "target_table": target_table,
            "metadata": metadata,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.operations.append(operation)

    def get_operations(self, path: Optional[str] = None) -> List[Dict]:
        """Retrieve tracked operations, optionally filtered by path."""
        if path is None:
            return self.operations
        return [op for op in self.operations if op.get("path") == path]


class UDFMapper:
    """Maps column lineage through user-defined functions."""

    def __init__(self):
        self.udfs = {}

    def register_udf(self, name: str, code: str, metadata: Dict) -> None:
        """Register a UDF for analysis."""
        self.udfs[name] = {
            "code": code,
            "metadata": metadata,
            "column_mappings": self._parse_column_mappings(code)
        }

    def _parse_column_mappings(self, code: str) -> List[str]:
        """Parse UDF code to extract column references."""
        pattern = r'\b(?:col|df|row)\[["\'](\w+)["\']\]'
        matches = re.findall(pattern, code)
        return list(set(matches))

    def get_column_mappings(self, udf_name: str) -> List[str]:
        """Get column mappings for a specific UDF."""
        if udf_name not in self.udfs:
            return []
        return self.udfs[udf_name]["column_mappings"]


class LineageTracker:
    """Main lineage tracking coordinator."""

    def __init__(self):
        self.unmanaged_capture = UnmanagedCapture()
        self.udf_mapper = UDFMapper()
        self.lineage_graph = []

    def record_lineage(self, source: str, target: str, columns: List[str],
                      operation: str) -> None:
        """Record a lineage relationship."""
        lineage_entry = {
            "source": source,
            "target": target,
            "columns": columns,
            "operation": operation
        }
        self.lineage_graph.append(lineage_entry)

    def get_lineage(self, entity: str) -> List[Dict]:
        """Get lineage for a specific entity."""
        return [entry for entry in self.lineage_graph
                if entry["source"] == entity or entry["target"] == entity]
