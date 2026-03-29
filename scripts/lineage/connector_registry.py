from typing import Dict, Optional, Callable


class BaseConnector:
    def __init__(self, config: Dict):
        self.config = config

    def extract_lineage(self, metadata: Dict) -> Dict:
        raise NotImplementedError("Subclasses must implement extract_lineage")


class PostgreSQLConnector(BaseConnector):
    def extract_lineage(self, metadata: Dict) -> Dict:
        tables = []
        for table in metadata.get("tables", []):
            tables.append({
                "schema": table.get("schema", "public"),
                "table": table["name"]
            })
        
        lineage = []
        for edge in metadata.get("dependencies", []):
            lineage.append({
                "source": f"postgres_{edge['source_schema']}_{edge['source_table']}",
                "target": f"postgres_{edge['target_schema']}_{edge['target_table']}",
                "transformation": edge.get("type", "unknown")
            })
        
        return {"tables": tables, "lineage": lineage}


class TableauConnector(BaseConnector):
    def extract_lineage(self, metadata: Dict) -> Dict:
        tables = []
        for dashboard in metadata.get("dashboards", []):
            tables.append({
                "schema": "dashboards",
                "table": dashboard["name"]
            })
        
        lineage = []
        for datasource in metadata.get("datasources", []):
            for target in datasource.get("used_in", []):
                lineage.append({
                    "source": datasource["id"],
                    "target": f"tableau_dashboards_{target}",
                    "transformation": "visualization"
                })
        
        return {"tables": tables, "lineage": lineage}


class ConnectorRegistry:
    def __init__(self):
        self.connectors = {}
        self._register_builtin_connectors()

    def _register_builtin_connectors(self):
        self.register("postgres", PostgreSQLConnector)
        self.register("tableau", TableauConnector)

    def register(self, platform: str, connector_class: type):
        self.connectors[platform] = connector_class

    def get_connector(self, platform: str, config: Optional[Dict] = None) -> Optional[BaseConnector]:
        connector_class = self.connectors.get(platform)
        if connector_class:
            return connector_class(config or {})
        return None

    def list_platforms(self) -> list:
        return list(self.connectors.keys())
