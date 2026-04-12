import networkx as nx
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import yaml

@dataclass
class LineageNode:
    system: str
    object_type: str
    full_name: str
    columns: List[str] = None
    metadata: Dict[str, Any] = None

class BaseConnector:
    def __init__(self, config: Dict):
        self.config = config
        self.system_name = config.get('system', 'unknown')

    def extract_lineage(self) -> List[Dict]:
        raise NotImplementedError

    def get_schema(self, object_name: str) -> List[str]:
        raise NotImplementedError

class SnowflakeConnector(BaseConnector):
    def extract_lineage(self) -> List[Dict]:
        return [
            {"source": f"{self.system_name}.raw.customers", "target": f"{self.system_name}.analytics.customers_clean", "type": "table"},
            {"source": f"{self.system_name}.analytics.customers_clean", "target": "databricks.corporate.companies", "type": "table"}
        ]

    def get_schema(self, object_name: str) -> List[str]:
        return ["id", "name", "email", "created_at"]

class TableauConnector(BaseConnector):
    def extract_lineage(self) -> List[Dict]:
        return [
            {"source": "databricks.corporate.companies", "target": f"{self.system_name}.dashboard.revenue_report", "type": "visualization"},
            {"source": "databricks.corporate.contracts", "target": f"{self.system_name}.dashboard.revenue_report", "type": "visualization"}
        ]

    def get_schema(self, object_name: str) -> List[str]:
        return ["company_name", "total_revenue", "contract_count"]

class LineageEngine:
    def __init__(self, config_path: Optional[str] = None):
        self.graph = nx.DiGraph()
        self.column_graph = nx.DiGraph()
        self.connectors = {}
        if config_path:
            self._load_config(config_path)

    def _load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            for conn_cfg in config.get('lineage_connectors', []):
                self.add_connector(conn_cfg['type'], conn_cfg)

    def add_connector(self, system_type: str, config: Dict):
        config['system'] = system_type
        if system_type == 'snowflake':
            self.connectors[system_type] = SnowflakeConnector(config)
        elif system_type == 'tableau':
            self.connectors[system_type] = TableauConnector(config)

    def build_unified_lineage(self) -> nx.DiGraph:
        for system, connector in self.connectors.items():
            edges = connector.extract_lineage()
            for edge in edges:
                self.graph.add_edge(edge['source'], edge['target'], type=edge['type'], system=system)
                src_cols = connector.get_schema(edge['source'])
                tgt_cols = connector.get_schema(edge['target'])
                for col in src_cols:
                    if col in tgt_cols:
                        self.column_graph.add_edge(f"{edge['source']}.{col}", f"{edge['target']}.{col}")
        return self.graph

    def query_lineage(self, object_name: str, direction: str = 'downstream') -> Dict:
        if direction == 'downstream':
            nodes = list(nx.descendants(self.graph, object_name)) if object_name in self.graph else []
        else:
            nodes = list(nx.ancestors(self.graph, object_name)) if object_name in self.graph else []
        return {"object": object_name, "direction": direction, "dependencies": nodes}

    def query_column_lineage(self, column_name: str) -> Dict:
        downstream = list(nx.descendants(self.column_graph, column_name)) if column_name in self.column_graph else []
        upstream = list(nx.ancestors(self.column_graph, column_name)) if column_name in self.column_graph else []
        return {"column": column_name, "upstream": upstream, "downstream": downstream}
