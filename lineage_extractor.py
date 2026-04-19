from unified_lineage import LineageGraph, LineageNode, LineageEdge


class ColumnNode(LineageNode):
    def __init__(self, node_id, column_name, dataframe, metadata=None):
        super().__init__(node_id, "column", metadata)
        self.column_name = column_name
        self.dataframe = dataframe

    def __repr__(self):
        return f"ColumnNode({self.column_name}, df={self.dataframe})"


class LineageExtractor:
    def __init__(self):
        self.graph = LineageGraph()

    def build_lineage(self, operations):
        for op in operations:
            if op['type'] == 'select':
                self._handle_select(op)
            elif op['type'] == 'join':
                self._handle_join(op)
            elif op['type'] == 'filter':
                self._handle_filter(op)
            elif op['type'] == 'transform':
                self._handle_transform(op)
        return self.graph

    def _handle_select(self, op):
        source_df = op['source']
        target_df = op['target']
        columns = op.get('columns', [])
        for col in columns:
            source_node = ColumnNode(f"{source_df}.{col}", col, source_df)
            target_node = ColumnNode(f"{target_df}.{col}", col, target_df)
            self.graph.add_node(source_node)
            self.graph.add_node(target_node)
            edge = LineageEdge(source_node.node_id, target_node.node_id, "select")
            self.graph.add_edge(edge)

    def _handle_join(self, op):
        left_df = op['left']
        right_df = op['right']
        target_df = op['target']
        on_col = op.get('on', 'id')
        left_node = ColumnNode(f"{left_df}.{on_col}", on_col, left_df)
        right_node = ColumnNode(f"{right_df}.{on_col}", on_col, right_df)
        target_node = ColumnNode(f"{target_df}.{on_col}", on_col, target_df)
        self.graph.add_node(left_node)
        self.graph.add_node(right_node)
        self.graph.add_node(target_node)
        self.graph.add_edge(LineageEdge(left_node.node_id, target_node.node_id, "join"))
        self.graph.add_edge(LineageEdge(right_node.node_id, target_node.node_id, "join"))

    def _handle_filter(self, op):
        source_df = op['source']
        target_df = op['target']
        columns = op.get('columns', [])
        for col in columns:
            source_node = ColumnNode(f"{source_df}.{col}", col, source_df)
            target_node = ColumnNode(f"{target_df}.{col}", col, target_df)
            self.graph.add_node(source_node)
            self.graph.add_node(target_node)
            self.graph.add_edge(LineageEdge(source_node.node_id, target_node.node_id, "filter"))

    def _handle_transform(self, op):
        source_df = op['source']
        target_df = op['target']
        source_col = op.get('source_column')
        target_col = op.get('target_column')
        source_node = ColumnNode(f"{source_df}.{source_col}", source_col, source_df)
        target_node = ColumnNode(f"{target_df}.{target_col}", target_col, target_df)
        self.graph.add_node(source_node)
        self.graph.add_node(target_node)
        self.graph.add_edge(LineageEdge(source_node.node_id, target_node.node_id, "transform"))
