import pytest
from scripts.lineage import LineageGraphBuilder, ConnectorRegistry, LineageStitcher


class TestLineageGraphBuilder:
    def test_add_table_node(self):
        builder = LineageGraphBuilder()
        builder.add_table_node("t1", "databricks", "sales", "orders")
        assert "t1" in builder.graph.nodes
        assert builder.graph.nodes["t1"]["type"] == "table"

    def test_add_lineage_edge(self):
        builder = LineageGraphBuilder()
        builder.add_table_node("t1", "databricks", "sales", "orders")
        builder.add_table_node("t2", "databricks", "analytics", "orders_agg")
        builder.add_lineage_edge("t1", "t2", "aggregation")
        assert builder.graph.has_edge("t1", "t2")

    def test_get_upstream(self):
        builder = LineageGraphBuilder()
        builder.add_table_node("t1", "postgres", "raw", "users")
        builder.add_table_node("t2", "databricks", "bronze", "users")
        builder.add_table_node("t3", "databricks", "silver", "users")
        builder.add_lineage_edge("t1", "t2")
        builder.add_lineage_edge("t2", "t3")
        upstream = builder.get_upstream("t3")
        assert "t1" in upstream
        assert "t2" in upstream

    def test_get_downstream(self):
        builder = LineageGraphBuilder()
        builder.add_table_node("t1", "postgres", "raw", "users")
        builder.add_table_node("t2", "databricks", "bronze", "users")
        builder.add_lineage_edge("t1", "t2")
        downstream = builder.get_downstream("t1")
        assert "t2" in downstream

    def test_impact_analysis(self):
        builder = LineageGraphBuilder()
        builder.add_table_node("t1", "databricks", "sales", "orders")
        builder.add_table_node("t2", "databricks", "analytics", "orders_agg")
        builder.add_lineage_edge("t1", "t2")
        impact = builder.get_impact_analysis("t1")
        assert impact["downstream_count"] == 1
        assert "t2" in impact["downstream_nodes"]


class TestConnectorRegistry:
    def test_builtin_connectors(self):
        registry = ConnectorRegistry()
        assert "postgres" in registry.list_platforms()
        assert "tableau" in registry.list_platforms()

    def test_get_connector(self):
        registry = ConnectorRegistry()
        connector = registry.get_connector("postgres")
        assert connector is not None

    def test_postgres_extract_lineage(self):
        registry = ConnectorRegistry()
        connector = registry.get_connector("postgres")
        metadata = {
            "tables": [{"name": "users", "schema": "public"}],
            "dependencies": []
        }
        result = connector.extract_lineage(metadata)
        assert len(result["tables"]) == 1
        assert result["tables"][0]["table"] == "users"


class TestLineageStitcher:
    def test_stitch_unity_catalog(self):
        registry = ConnectorRegistry()
        stitcher = LineageStitcher(registry)
        uc_lineage = {
            "tables": [{
                "catalog": "main",
                "schema": "sales",
                "table": "orders",
                "columns": [{"name": "id", "type": "int"}]
            }],
            "lineage": []
        }
        stitcher.stitch_unity_catalog(uc_lineage)
        assert len(stitcher.graph_builder.graph.nodes) > 0

    def test_link_cross_platform(self):
        registry = ConnectorRegistry()
        stitcher = LineageStitcher(registry)
        stitcher.graph_builder.add_table_node("t1", "postgres", "public", "users")
        stitcher.graph_builder.add_table_node("t2", "databricks", "bronze", "users")
        stitcher.link_cross_platform("t1", "t2", "etl")
        assert stitcher.graph_builder.graph.has_edge("t1", "t2")

    def test_get_end_to_end_lineage(self):
        registry = ConnectorRegistry()
        stitcher = LineageStitcher(registry)
        stitcher.graph_builder.add_table_node("t1", "postgres", "public", "users")
        stitcher.graph_builder.add_table_node("t2", "databricks", "bronze", "users")
        stitcher.graph_builder.add_lineage_edge("t1", "t2")
        lineage = stitcher.get_end_to_end_lineage("t2")
        assert "postgres" in lineage["platforms_involved"]
        assert "databricks" in lineage["platforms_involved"]
