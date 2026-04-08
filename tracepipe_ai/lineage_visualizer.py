from typing import Dict, Any, List
import json

class LineageVisualizer:
    def generate_graph(self, lineage_data: Dict[str, Any], format: str = "json") -> Any:
        if format == "json":
            return self._generate_json_graph(lineage_data)
        elif format == "mermaid":
            return self._generate_mermaid_graph(lineage_data)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _generate_json_graph(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        target_table = lineage_data.get("target_table", "unknown")
        nodes.append({"id": target_table, "type": "table", "label": target_table})
        
        for source_table in lineage_data.get("source_tables", []):
            nodes.append({"id": source_table, "type": "table", "label": source_table})
        
        for col_name, col_info in lineage_data.get("columns", {}).items():
            target_col_id = f"{target_table}.{col_name}"
            nodes.append({"id": target_col_id, "type": "column", "label": col_name})
            edges.append({"from": target_table, "to": target_col_id, "type": "contains"})
            
            for source_col in col_info.get("source_columns", []):
                for source_table in lineage_data.get("source_tables", []):
                    source_col_id = f"{source_table}.{source_col}"
                    if not any(n["id"] == source_col_id for n in nodes):
                        nodes.append({"id": source_col_id, "type": "column", "label": source_col})
                        edges.append({"from": source_table, "to": source_col_id, "type": "contains"})
                    edges.append({
                        "from": source_col_id,
                        "to": target_col_id,
                        "type": "transformation",
                        "transformation_type": col_info.get("transformation_type", "unknown")
                    })
        
        return {"nodes": nodes, "edges": edges}

    def _generate_mermaid_graph(self, lineage_data: Dict[str, Any]) -> str:
        lines = ["graph LR"]
        target_table = lineage_data.get("target_table", "unknown")
        
        for source_table in lineage_data.get("source_tables", []):
            lines.append(f"    {source_table}[{source_table}]")
        
        lines.append(f"    {target_table}[{target_table}]")
        
        for col_name, col_info in lineage_data.get("columns", {}).items():
            transform_type = col_info.get("transformation_type", "unknown")
            for source_col in col_info.get("source_columns", []):
                for source_table in lineage_data.get("source_tables", []):
                    lines.append(f"    {source_table} -->|{source_col} -> {col_name} ({transform_type})| {target_table}")
        
        return "\n".join(lines)
