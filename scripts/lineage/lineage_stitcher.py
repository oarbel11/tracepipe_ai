from typing import Dict, List, Optional
from .lineage_graph import LineageGraphBuilder
from .connector_registry import ConnectorRegistry


class LineageStitcher:
    def __init__(self, registry: ConnectorRegistry):
        self.registry = registry
        self.graph_builder = LineageGraphBuilder()

    def stitch_unity_catalog(self, uc_lineage: Dict):
        for table in uc_lineage.get("tables", []):
            node_id = f"uc_{table['catalog']}_{table['schema']}_{table['table']}"
            self.graph_builder.add_table_node(
                node_id, "unity_catalog", table["schema"], table["table"],
                {"catalog": table["catalog"]}
            )
            for col in table.get("columns", []):
                col_id = f"{node_id}_col_{col['name']}"
                self.graph_builder.add_column_node(
                    col_id, node_id, col["name"], col.get("type", "unknown")
                )

        for edge in uc_lineage.get("lineage", []):
            self.graph_builder.add_lineage_edge(
                edge["source"], edge["target"], edge.get("transformation")
            )

    def stitch_external_source(self, platform: str, metadata: Dict):
        connector = self.registry.get_connector(platform)
        if not connector:
            raise ValueError(f"No connector for platform: {platform}")

        lineage_data = connector.extract_lineage(metadata)
        
        for table in lineage_data.get("tables", []):
            node_id = f"{platform}_{table['schema']}_{table['table']}"
            self.graph_builder.add_table_node(
                node_id, platform, table["schema"], table["table"]
            )

        for edge in lineage_data.get("lineage", []):
            self.graph_builder.add_lineage_edge(
                edge["source"], edge["target"], edge.get("transformation")
            )

    def link_cross_platform(self, source_node: str, target_node: str,
                           link_type: str = "synchronized"):
        self.graph_builder.add_lineage_edge(source_node, target_node, link_type)

    def auto_detect_links(self, matching_strategy: str = "name_match"):
        nodes = self.graph_builder.graph.nodes(data=True)
        tables = [(nid, data) for nid, data in nodes if data.get("type") == "table"]
        
        linked = []
        for i, (id1, data1) in enumerate(tables):
            for id2, data2 in tables[i+1:]:
                if data1["platform"] == data2["platform"]:
                    continue
                if matching_strategy == "name_match":
                    if (data1.get("table") == data2.get("table") and
                        data1.get("schema") == data2.get("schema")):
                        self.link_cross_platform(id1, id2, "auto_linked")
                        linked.append((id1, id2))
        return linked

    def get_end_to_end_lineage(self, node_id: str) -> Dict:
        upstream = self.graph_builder.get_upstream(node_id)
        downstream = self.graph_builder.get_downstream(node_id)
        
        platforms = set()
        for nid in upstream + downstream + [node_id]:
            if nid in self.graph_builder.graph:
                platform = self.graph_builder.graph.nodes[nid].get("platform")
                if platform:
                    platforms.add(platform)
        
        return {
            "node": node_id,
            "upstream": upstream,
            "downstream": downstream,
            "platforms_involved": list(platforms),
            "total_nodes": len(upstream) + len(downstream) + 1
        }
