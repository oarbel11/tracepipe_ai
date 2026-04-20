from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, field
import re
import ast

@dataclass
class DataFlowNode:
    node_id: str
    operation_type: str
    input_columns: Set[str] = field(default_factory=set)
    output_columns: Set[str] = field(default_factory=set)
    transformation_logic: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

class AdvancedSparkAnalyzer:
    def __init__(self):
        self.flow_graph: Dict[str, DataFlowNode] = {}
        self.udf_registry: Dict[str, Dict] = {}
        self.operation_handlers = {
            'map': self._analyze_map,
            'filter': self._analyze_filter,
            'groupBy': self._analyze_groupby,
            'agg': self._analyze_agg,
            'withColumn': self._analyze_withcolumn,
            'select': self._analyze_select
        }

    def register_udf(self, udf_name: str, params: List[str], return_cols: List[str]):
        self.udf_registry[udf_name] = {'params': params, 'returns': return_cols}

    def _analyze_map(self, code: str, node_id: str) -> DataFlowNode:
        func_match = re.search(r'map\(lambda\s+(\w+):\s*(.+?)\)', code)
        if func_match:
            param, body = func_match.groups()
            return DataFlowNode(node_id, 'map', {param}, set(), body)
        return DataFlowNode(node_id, 'map')

    def _analyze_filter(self, code: str, node_id: str) -> DataFlowNode:
        col_refs = re.findall(r'col\(["\'](.+?)["\']\)', code)
        return DataFlowNode(node_id, 'filter', set(col_refs), set(col_refs))

    def _analyze_groupby(self, code: str, node_id: str) -> DataFlowNode:
        group_cols = re.findall(r'groupBy\((.+?)\)', code)
        if group_cols:
            cols = [c.strip().strip('"\' ') for c in group_cols[0].split(',')]
            return DataFlowNode(node_id, 'groupBy', set(cols), set(cols))
        return DataFlowNode(node_id, 'groupBy')

    def _analyze_agg(self, code: str, node_id: str) -> DataFlowNode:
        agg_funcs = re.findall(r'(sum|avg|count|max|min)\(["\'](.+?)["\']\)', code)
        input_cols = {col for _, col in agg_funcs}
        output_cols = {f"{func}_{col}" for func, col in agg_funcs}
        return DataFlowNode(node_id, 'agg', input_cols, output_cols)

    def _analyze_withcolumn(self, code: str, node_id: str) -> DataFlowNode:
        match = re.search(r'withColumn\(["\'](.+?)["\'],\s*(.+?)\)', code)
        if match:
            new_col, expr = match.groups()
            input_cols = set(re.findall(r'col\(["\'](.+?)["\']\)', expr))
            return DataFlowNode(node_id, 'withColumn', input_cols, {new_col}, expr)
        return DataFlowNode(node_id, 'withColumn')

    def _analyze_select(self, code: str, node_id: str) -> DataFlowNode:
        cols = re.findall(r'select\((.+?)\)', code)
        if cols:
            col_list = [c.strip().strip('"\' ') for c in cols[0].split(',')]
            return DataFlowNode(node_id, 'select', set(col_list), set(col_list))
        return DataFlowNode(node_id, 'select')

    def analyze_operation(self, code: str, op_type: str, node_id: str) -> DataFlowNode:
        handler = self.operation_handlers.get(op_type, lambda c, n: DataFlowNode(n, op_type))
        node = handler(code, node_id)
        self.flow_graph[node_id] = node
        return node

    def trace_column_lineage(self, target_col: str) -> List[DataFlowNode]:
        lineage = []
        for node in self.flow_graph.values():
            if target_col in node.output_columns or target_col in node.input_columns:
                lineage.append(node)
        return lineage
