from typing import Dict, List, Set, Optional
import re
from dataclasses import dataclass, field

@dataclass
class TransformationNode:
    operation: str
    input_columns: Set[str] = field(default_factory=set)
    output_columns: Set[str] = field(default_factory=set)
    metadata: Dict = field(default_factory=dict)

class AdvancedTransformTracker:
    def __init__(self):
        self.transformations: List[TransformationNode] = []
        self.complex_ops = {
            'explode': self._track_explode,
            'pivot': self._track_pivot,
            'groupBy': self._track_groupby,
            'join': self._track_join,
            'union': self._track_union,
        }
    
    def track_transformation(self, code: str) -> List[TransformationNode]:
        for op_name, tracker_func in self.complex_ops.items():
            pattern = rf'\.{op_name}\(([^)]+)\)'
            for match in re.finditer(pattern, code, re.IGNORECASE):
                node = tracker_func(match.group(1))
                if node:
                    self.transformations.append(node)
        return self.transformations
    
    def _track_explode(self, args: str) -> Optional[TransformationNode]:
        col_match = re.search(r'["\']([^"\')]+)["\']', args)
        if col_match:
            col = col_match.group(1)
            return TransformationNode(
                operation='explode',
                input_columns={col},
                output_columns={col + '_exploded'},
                metadata={'type': 'array_expansion'}
            )
        return None
    
    def _track_pivot(self, args: str) -> Optional[TransformationNode]:
        col_match = re.search(r'["\']([^"\')]+)["\']', args)
        if col_match:
            pivot_col = col_match.group(1)
            return TransformationNode(
                operation='pivot',
                input_columns={pivot_col},
                output_columns=set(),
                metadata={'pivot_column': pivot_col}
            )
        return None
    
    def _track_groupby(self, args: str) -> Optional[TransformationNode]:
        cols = [c.strip().strip('"\' ') for c in args.split(',')]
        return TransformationNode(
            operation='groupBy',
            input_columns=set(cols),
            output_columns=set(cols),
            metadata={'aggregation': True}
        )
    
    def _track_join(self, args: str) -> Optional[TransformationNode]:
        return TransformationNode(
            operation='join',
            input_columns=set(),
            output_columns=set(),
            metadata={'type': 'join'}
        )
    
    def _track_union(self, args: str) -> Optional[TransformationNode]:
        return TransformationNode(
            operation='union',
            input_columns=set(),
            output_columns=set(),
            metadata={'type': 'union'}
        )
    
    def get_transformation_chain(self) -> List[Dict]:
        return [{
            'operation': t.operation,
            'inputs': list(t.input_columns),
            'outputs': list(t.output_columns),
            'metadata': t.metadata
        } for t in self.transformations]
