import duckdb
import pandas as pd
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ChangeImpact:
    change_type: str
    affected_object: str
    object_type: str
    severity: str
    downstream_count: int
    details: Dict[str, Any]

class ChangeSimulator:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.impact_cache = {}

    def simulate_schema_change(self, table: str, changes: Dict[str, Any]) -> List[ChangeImpact]:
        impacts = []
        downstream = self._get_downstream_objects(table)
        
        for change_type, change_data in changes.items():
            if change_type == 'drop_column':
                impacts.extend(self._analyze_column_drop(table, change_data, downstream))
            elif change_type == 'type_change':
                impacts.extend(self._analyze_type_change(table, change_data, downstream))
            elif change_type == 'add_constraint':
                impacts.extend(self._analyze_constraint_add(table, change_data, downstream))
        
        return impacts

    def _get_downstream_objects(self, table: str) -> Dict[str, List[str]]:
        downstream = {'tables': [], 'views': [], 'models': []}
        
        try:
            views = self.conn.execute(
                "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_type='VIEW'"
            ).fetchall()
            downstream['views'] = [v[0] for v in views]
        except:
            pass
        
        return downstream

    def _analyze_column_drop(self, table: str, column: str, downstream: Dict) -> List[ChangeImpact]:
        impacts = []
        severity = 'high' if len(downstream['views']) > 5 else 'medium'
        
        impacts.append(ChangeImpact(
            change_type='drop_column',
            affected_object=table,
            object_type='table',
            severity=severity,
            downstream_count=sum(len(v) for v in downstream.values()),
            details={'column': column, 'downstream': downstream}
        ))
        return impacts

    def _analyze_type_change(self, table: str, change: Dict, downstream: Dict) -> List[ChangeImpact]:
        impacts = []
        column, old_type, new_type = change['column'], change['old_type'], change['new_type']
        
        severity = 'critical' if not self._is_compatible_type(old_type, new_type) else 'low'
        
        impacts.append(ChangeImpact(
            change_type='type_change',
            affected_object=table,
            object_type='table',
            severity=severity,
            downstream_count=sum(len(v) for v in downstream.values()),
            details={'column': column, 'old_type': old_type, 'new_type': new_type}
        ))
        return impacts

    def _analyze_constraint_add(self, table: str, constraint: Dict, downstream: Dict) -> List[ChangeImpact]:
        return [ChangeImpact(
            change_type='add_constraint',
            affected_object=table,
            object_type='table',
            severity='medium',
            downstream_count=sum(len(v) for v in downstream.values()),
            details=constraint
        )]

    def _is_compatible_type(self, old_type: str, new_type: str) -> bool:
        compatible_pairs = [('INTEGER', 'BIGINT'), ('FLOAT', 'DOUBLE')]
        return (old_type, new_type) in compatible_pairs
