import json
from typing import Dict, List, Optional
from pathlib import Path
from dataclasses import asdict
from .lineage_parser import ColumnLineageParser, ColumnLineage
from .udf_analyzer import UDFAnalyzer

class LineageTracker:
    def __init__(self, storage_path: str = "lineage_metadata.json"):
        self.storage_path = Path(storage_path)
        self.parser = ColumnLineageParser()
        self.udf_analyzer = UDFAnalyzer()
        self.lineage_data = self._load_metadata()
        self.manual_overrides = {}

    def _load_metadata(self) -> Dict:
        if self.storage_path.exists():
            with open(self.storage_path, 'r') as f:
                return json.load(f)
        return {"tables": {}, "columns": {}, "udfs": {}}

    def _save_metadata(self):
        with open(self.storage_path, 'w') as f:
            json.dump(self.lineage_data, f, indent=2)

    def track_sql_lineage(self, sql: str, target_table: str) -> List[ColumnLineage]:
        lineages = self.parser.parse_sql(sql)
        self._store_lineage(target_table, lineages)
        return lineages

    def track_path_based_source(self, path: str, table_name: str, schema: Dict[str, str]):
        if table_name not in self.lineage_data["tables"]:
            self.lineage_data["tables"][table_name] = {}
        self.lineage_data["tables"][table_name]["path"] = path
        self.lineage_data["tables"][table_name]["schema"] = schema
        self._save_metadata()

    def register_udf_lineage(self, udf_name: str, code: str):
        lineage = self.udf_analyzer.analyze_udf(code)
        self.lineage_data["udfs"][udf_name] = lineage
        self._save_metadata()

    def add_manual_lineage(self, target_table: str, target_col: str, 
                          source_mappings: List[Dict[str, str]]):
        key = f"{target_table}.{target_col}"
        self.manual_overrides[key] = source_mappings
        if target_table not in self.lineage_data["columns"]:
            self.lineage_data["columns"][target_table] = {}
        self.lineage_data["columns"][target_table][target_col] = {
            "manual": True,
            "sources": source_mappings
        }
        self._save_metadata()

    def _store_lineage(self, target_table: str, lineages: List[ColumnLineage]):
        if target_table not in self.lineage_data["columns"]:
            self.lineage_data["columns"][target_table] = {}
        for lin in lineages:
            key = f"{target_table}.{lin.target_column}"
            if key not in self.manual_overrides:
                self.lineage_data["columns"][target_table][lin.target_column] = asdict(lin)
        self._save_metadata()

    def get_column_lineage(self, table: str, column: str) -> Optional[Dict]:
        return self.lineage_data.get("columns", {}).get(table, {}).get(column)

    def get_impact_analysis(self, source_table: str, source_column: str) -> List[str]:
        impacted = []
        for table, cols in self.lineage_data.get("columns", {}).items():
            for col, lineage in cols.items():
                sources = lineage.get("source_columns", [])
                for src in sources:
                    if isinstance(src, list) and len(src) == 2:
                        if src[0] == source_table and src[1] == source_column:
                            impacted.append(f"{table}.{col}")
        return impacted
