class ColumnNode:
    def __init__(self, column, dataframe=None, expression=None):
        self.column = column
        self.dataframe = dataframe
        self.expression = expression
        self.dependencies = []

    def __repr__(self):
        return f"ColumnNode({self.column}, df={self.dataframe})"


class LineageExtractor:
    def __init__(self):
        self.lineage = {}

    def extract_from_plan(self, plan_str):
        lines = plan_str.strip().split('\n')
        nodes = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if 'Project' in line:
                cols = self._extract_columns(line)
                for col in cols:
                    node = ColumnNode(col, dataframe='result')
                    nodes[col] = node
            elif 'Relation' in line:
                parts = line.split()
                for part in parts:
                    if part.startswith('[') and part.endswith(']'):
                        cols = part[1:-1].split(',')
                        for col in cols:
                            col = col.strip()
                            if col:
                                node = ColumnNode(col, dataframe='source')
                                nodes[col] = node
        
        self.lineage = nodes
        return nodes

    def build_lineage(self, dataframe):
        if hasattr(dataframe, '_plan_str'):
            return self.extract_from_plan(dataframe._plan_str)
        return {}

    def _extract_columns(self, line):
        cols = []
        if '[' in line and ']' in line:
            start = line.index('[')
            end = line.index(']')
            col_str = line[start+1:end]
            cols = [c.strip() for c in col_str.split(',') if c.strip()]
        return cols

    def get_column_lineage(self, column):
        if column in self.lineage:
            return self.lineage[column]
        return None

    def get_all_columns(self):
        return list(self.lineage.keys())
