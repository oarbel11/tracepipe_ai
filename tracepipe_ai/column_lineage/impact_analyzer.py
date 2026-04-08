from typing import Dict, List, Set

class ImpactAnalyzer:
    def __init__(self, workspace_client=None):
        self.workspace_client = workspace_client
        self.lineage_graph = {}
    
    def build_graph(self, lineage_data: List[Dict]):
        self.lineage_graph = {}
        for item in lineage_data:
            table = item.get('table')
            columns = item.get('columns', {})
            
            if table not in self.lineage_graph:
                self.lineage_graph[table] = {}
            
            for col_name, col_info in columns.items():
                self.lineage_graph[table][col_name] = col_info
    
    def analyze_impact(self, table: str, column: str) -> Dict:
        if table not in self.lineage_graph:
            return {"impacted_columns": [], "impacted_tables": []}
        
        impacted = set()
        impacted_tables = set()
        self._find_downstream(table, column, impacted, impacted_tables)
        
        return {
            "impacted_columns": sorted(list(impacted)),
            "impacted_tables": sorted(list(impacted_tables))
        }
    
    def _find_downstream(self, table: str, column: str, 
                        impacted: Set, tables: Set):
        for tbl, cols in self.lineage_graph.items():
            if tbl == table:
                continue
            
            for col_name, col_info in cols.items():
                sources = col_info.get('source_columns', [])
                if column in sources:
                    key = f"{tbl}.{col_name}"
                    if key not in impacted:
                        impacted.add(key)
                        tables.add(tbl)
                        self._find_downstream(tbl, col_name, impacted, tables)
