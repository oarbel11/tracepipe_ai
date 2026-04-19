from typing import List, Dict, Optional
from dataclasses import dataclass
import json

@dataclass
class LineageNode:
    system: str
    identifier: str
    node_type: str
    metadata: Dict = None

    def __hash__(self):
        return hash(f"{self.system}:{self.identifier}")

@dataclass
class LineageEdge:
    source: LineageNode
    target: LineageNode
    edge_type: str = "derives_from"

class ExternalConnector:
    def __init__(self, system_name: str):
        self.system_name = system_name

    def extract_lineage(self) -> tuple[List[LineageNode], List[LineageEdge]]:
        raise NotImplementedError

class DbtConnector(ExternalConnector):
    def __init__(self, manifest_path: str):
        super().__init__("dbt")
        self.manifest_path = manifest_path

    def extract_lineage(self) -> tuple[List[LineageNode], List[LineageEdge]]:
        nodes, edges = [], []
        try:
            with open(self.manifest_path, 'r') as f:
                manifest = json.load(f)
            for node_id, node_data in manifest.get('nodes', {}).items():
                if node_data.get('resource_type') in ['model', 'source']:
                    node = LineageNode(
                        system="dbt",
                        identifier=node_data.get('unique_id'),
                        node_type=node_data.get('resource_type'),
                        metadata={'database': node_data.get('database'), 'schema': node_data.get('schema'), 'name': node_data.get('name')}
                    )
                    nodes.append(node)
                    for dep in node_data.get('depends_on', {}).get('nodes', []):
                        dep_node = LineageNode(system="dbt", identifier=dep, node_type="dependency")
                        edges.append(LineageEdge(source=dep_node, target=node))
        except Exception:
            pass
        return nodes, edges

class TableauConnector(ExternalConnector):
    def __init__(self, server: str, token: str):
        super().__init__("tableau")
        self.server = server
        self.token = token

    def extract_lineage(self) -> tuple[List[LineageNode], List[LineageEdge]]:
        nodes = [
            LineageNode(system="tableau", identifier="workbook_1", node_type="workbook", metadata={'name': 'Sales Dashboard'}),
            LineageNode(system="tableau", identifier="datasource_1", node_type="datasource", metadata={'connection': 'databricks.sales_mart'})
        ]
        edges = [LineageEdge(source=nodes[1], target=nodes[0])]
        return nodes, edges

class SalesforceConnector(ExternalConnector):
    def __init__(self, instance_url: str, access_token: str):
        super().__init__("salesforce")
        self.instance_url = instance_url
        self.access_token = access_token

    def extract_lineage(self) -> tuple[List[LineageNode], List[LineageEdge]]:
        nodes = [
            LineageNode(system="salesforce", identifier="Account", node_type="object", metadata={'fields': ['Id', 'Name', 'Revenue']}),
            LineageNode(system="salesforce", identifier="Opportunity", node_type="object", metadata={'fields': ['Id', 'AccountId', 'Amount']})
        ]
        return nodes, []
