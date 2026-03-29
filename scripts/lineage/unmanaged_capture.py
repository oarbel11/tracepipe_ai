"""Capture lineage for unmanaged files and tables."""

import re
import time
from typing import Dict, List, Any, Optional


class UnmanagedLineageCapture:
    """Captures lineage for unmanaged file operations."""

    WRITE_PATTERNS = [
        r"\\.save\\s*\\(['\"](?!delta|jdbc)[^'\"]+['\"]\\)",
        r"\\.parquet\\s*\\(['\"][^'\"]+['\"]\\)",
        r"\\.csv\\s*\\(['\"][^'\"]+['\"]\\)",
        r"\\.json\\s*\\(['\"][^'\"]+['\"]\\)"
    ]

    def __init__(self):
        self.captured_operations: List[Dict[str, Any]] = []

    def capture_write_operation(self, code: str, context: Dict[str, Any]
                                ) -> Optional[Dict[str, Any]]:
        """Capture write operations from code."""
        for pattern in self.WRITE_PATTERNS:
            match = re.search(pattern, code)
            if match:
                operation = {
                    'timestamp': time.time(),
                    'operation_type': 'write',
                    'target_path': match.group(0),
                    'source_tables': context.get('source_tables', []),
                    'columns': context.get('columns', [])
                }
                self.captured_operations.append(operation)
                return operation
        return None

    def get_lineage(self, path: str) -> List[Dict[str, Any]]:
        """Get lineage for a specific path."""
        return [
            op for op in self.captured_operations
            if path in op.get('target_path', '')
        ]

    def reset(self):
        """Reset captured operations."""
        self.captured_operations = []
