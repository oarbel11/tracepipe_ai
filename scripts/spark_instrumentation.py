import ast
import inspect
from typing import Dict, List, Any, Optional, Set
import re

class SparkASTAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.lineage_ops = []
        self.udf_calls = []
        self.dynamic_sql = []
        self.dataframe_ops = []

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in ['select', 'withColumn', 'join', 'filter', 'groupBy']:
                self.dataframe_ops.append({'op': method, 'args': self._extract_args(node)})
            elif method == 'sql':
                self.dynamic_sql.append(self._extract_sql_string(node))
        elif isinstance(node.func, ast.Name) and 'udf' in node.func.id:
            self.udf_calls.append(node.func.id)
        self.generic_visit(node)

    def _extract_args(self, node):
        args = []
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                args.append(arg.value)
            elif isinstance(arg, ast.Name):
                args.append(arg.id)
        return args

    def _extract_sql_string(self, node):
        if node.args and isinstance(node.args[0], ast.Constant):
            return node.args[0].value
        return None

class InstrumentedLineageExtractor:
    def __init__(self):
        self.runtime_lineage = []
        self.captured_operations = []

    def extract_from_notebook(self, notebook_code: str) -> Dict[str, Any]:
        try:
            tree = ast.parse(notebook_code)
            analyzer = SparkASTAnalyzer()
            analyzer.visit(tree)
            lineage = self._build_lineage_graph(analyzer)
            return lineage
        except SyntaxError:
            return {'error': 'Failed to parse notebook', 'operations': []}

    def _build_lineage_graph(self, analyzer: SparkASTAnalyzer) -> Dict[str, Any]:
        nodes = set()
        edges = []
        for op in analyzer.dataframe_ops:
            for arg in op['args']:
                if isinstance(arg, str):
                    nodes.add(arg)
                    edges.append({'source': arg, 'target': op['op'], 'type': 'transform'})
        for sql in analyzer.dynamic_sql:
            if sql:
                tables = self._extract_tables_from_sql(sql)
                nodes.update(tables)
                for table in tables:
                    edges.append({'source': table, 'target': 'dynamic_sql', 'type': 'query'})
        return {'nodes': list(nodes), 'edges': edges, 'udf_calls': analyzer.udf_calls}

    def _extract_tables_from_sql(self, sql: str) -> Set[str]:
        pattern = r'FROM\s+([\w.]+)|JOIN\s+([\w.]+)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        tables = set()
        for match in matches:
            tables.update([t for t in match if t])
        return tables

    def instrument_udf(self, udf_func):
        def wrapper(*args, **kwargs):
            self.captured_operations.append({'udf': udf_func.__name__, 'args': args})
            return udf_func(*args, **kwargs)
        return wrapper
