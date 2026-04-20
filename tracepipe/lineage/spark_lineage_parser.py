"""Spark lineage parser for column-level tracking."""

import ast
from typing import Dict, List, Set
from tracepipe.lineage.udf_analyzer import UDFAnalyzer
from tracepipe.lineage.dataframe_tracker import DataFrameTracker


class SparkLineageParser:
    """Parse Spark code to extract column-level lineage."""

    def __init__(self):
        self.udf_analyzer = UDFAnalyzer()
        self.df_tracker = DataFrameTracker()

    def parse_code(self, code: str) -> Dict[str, List[Dict]]:
        """Parse Python/Scala code and return column lineage."""
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return {"lineage": [], "udfs": [], "errors": ["Syntax error"]}

        lineage = []
        udfs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                udf_info = self.udf_analyzer.analyze_udf(node)
                if udf_info:
                    udfs.append(udf_info)

            elif isinstance(node, ast.Call):
                df_lineage = self.df_tracker.track_operation(node)
                if df_lineage:
                    lineage.extend(df_lineage)

        return {"lineage": lineage, "udfs": udfs, "errors": []}

    def get_column_dependencies(self, code: str, target_col: str) -> Set[str]:
        """Get all source columns that contribute to target column."""
        result = self.parse_code(code)
        deps = set()

        for lin in result["lineage"]:
            if lin.get("target") == target_col:
                deps.update(lin.get("sources", []))

        return deps
