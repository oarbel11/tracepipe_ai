from typing import Dict, List, Set, Optional

class ColumnNode:
    def __init__(self, dataframe: str, column: str):
        self.dataframe = dataframe
        self.column = column
        self.dependencies = []

    def __repr__(self):
        return f"{self.dataframe}.{self.column}"

    def __eq__(self, other):
        if not isinstance(other, ColumnNode):
            return False
        return self.dataframe == other.dataframe and self.column == other.column

    def __hash__(self):
        return hash((self.dataframe, self.column))

class LineageExtractor:
    def __init__(self, parsed_data: Dict):
        self.parsed_data = parsed_data
        self.nodes = {}
        self.edges = []

    def build_lineage(self) -> Dict[str, List[ColumnNode]]:
        operations = self.parsed_data.get('operations', [])
        for op in operations:
            self._process_operation(op)
        return self._build_lineage_dict()

    def _process_operation(self, op: Dict):
        target_df = op['target']
        source_df = op.get('source')
        operation = op['operation']
        columns = op.get('columns', [])

        if operation == 'select':
            for col in columns:
                target_node = self._get_or_create_node(target_df, col)
                if source_df:
                    source_node = self._get_or_create_node(source_df, col)
                    target_node.dependencies.append(source_node)
                    self.edges.append((source_node, target_node))
        elif operation == 'withColumn':
            if columns:
                target_col = columns[0]
                target_node = self._get_or_create_node(target_df, target_col)
                if source_df:
                    for col in columns[1:]:
                        source_node = self._get_or_create_node(source_df, col)
                        target_node.dependencies.append(source_node)
                        self.edges.append((source_node, target_node))

    def _get_or_create_node(self, df: str, col: str) -> ColumnNode:
        key = f"{df}.{col}"
        if key not in self.nodes:
            self.nodes[key] = ColumnNode(df, col)
        return self.nodes[key]

    def _build_lineage_dict(self) -> Dict[str, List[ColumnNode]]:
        result = {}
        for key, node in self.nodes.items():
            result[key] = node.dependencies
        return result

    def get_upstream_columns(self, df: str, col: str) -> List[ColumnNode]:
        node = self.nodes.get(f"{df}.{col}")
        if not node:
            return []
        return self._get_all_upstream(node, set())

    def _get_all_upstream(self, node: ColumnNode, visited: Set) -> List[ColumnNode]:
        if node in visited:
            return []
        visited.add(node)
        result = list(node.dependencies)
        for dep in node.dependencies:
            result.extend(self._get_all_upstream(dep, visited))
        return result
