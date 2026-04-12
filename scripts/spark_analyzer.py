from typing import Dict, List, Optional
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.lineage_extractor import LineageExtractor
import os

class SparkAnalyzer:
    def __init__(self, catalog: str = "companies_data"):
        self.catalog = catalog
        self.parser = SparkLineageParser()
        self.extractor = LineageExtractor()

    def analyze_file(self, filepath: str, source_table: str = "source",
                     target_table: str = "target") -> Dict:
        self.parser.parse_file(filepath)
        return self._build_lineage(source_table, target_table)

    def analyze_code(self, code: str, source_table: str = "source",
                     target_table: str = "target") -> Dict:
        self.parser.parse_code(code)
        return self._build_lineage(source_table, target_table)

    def _build_lineage(self, source_table: str, target_table: str) -> Dict:
        for op in self.parser.operations:
            for inp in op.inputs:
                for out in op.outputs:
                    self.extractor.add_transformation(
                        source_table, inp, target_table, out, op.op_type
                    )
        return {
            'operations_count': len(self.parser.operations),
            'udfs_count': len(self.parser.udfs),
            'column_lineage': self.parser.get_column_lineage(),
            'lineage_graph': self.extractor.export_lineage(),
            'udfs': {name: udf.params for name, udf in self.parser.udfs.items()}
        }

    def get_column_dependencies(self, table: str, column: str) -> Dict:
        return self.extractor.get_column_impact(table, column)

    def trace_column_origin(self, source_table: str, source_col: str,
                           target_table: str, target_col: str) -> List:
        paths = self.extractor.get_lineage_path(
            source_table, source_col, target_table, target_col
        )
        result = []
        for path in paths:
            result.append([f"{n.table}.{n.column}" for n in path])
        return result

    def analyze_directory(self, directory: str, pattern: str = "*.py") -> Dict:
        import glob
        total_ops = 0
        total_udfs = 0
        all_lineage = []
        for filepath in glob.glob(os.path.join(directory, pattern)):
            result = self.analyze_file(filepath)
            total_ops += result['operations_count']
            total_udfs += result['udfs_count']
            all_lineage.extend(result['column_lineage'])
        return {
            'files_analyzed': len(glob.glob(os.path.join(directory, pattern))),
            'total_operations': total_ops,
            'total_udfs': total_udfs,
            'total_lineage_edges': len(all_lineage)
        }
