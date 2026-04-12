import unittest
import json
from tracepipe_ai.lineage_integration import (
    SnowflakeConnector, TableauConnector, LineageNode, LineageEdge
)
from tracepipe_ai.lineage_stitcher import LineageStitcher


class TestLineageIntegration(unittest.TestCase):
    def test_snowflake_connector(self):
        config = {
            "system": "snowflake",
            "tables": [{"name": "customers", "schema": "public"}]
        }
        connector = SnowflakeConnector(config)
        graph = connector.extract_lineage()
        self.assertEqual(len(graph.nodes), 1)
        self.assertIn("snowflake:customers", graph.nodes)

    def test_tableau_connector(self):
        config = {
            "system": "tableau",
            "workbooks": [{"name": "sales_dashboard"}]
        }
        connector = TableauConnector(config)
        graph = connector.extract_lineage()
        self.assertEqual(len(graph.nodes), 1)
        self.assertIn("tableau:sales_dashboard", graph.nodes)

    def test_lineage_stitcher(self):
        sf_config = {
            "system": "snowflake",
            "tables": [{"name": "customers", "schema": "public"}]
        }
        tb_config = {
            "system": "tableau",
            "workbooks": [{"name": "sales_dashboard"}]
        }
        sf_connector = SnowflakeConnector(sf_config)
        tb_connector = TableauConnector(tb_config)
        sf_graph = sf_connector.extract_lineage()
        tb_graph = tb_connector.extract_lineage()

        stitcher = LineageStitcher()
        mappings = [{
            "source": "snowflake:customers",
            "target": "tableau:sales_dashboard"
        }]
        unified = stitcher.stitch([sf_graph, tb_graph], mappings)
        self.assertEqual(len(unified.nodes), 2)
        self.assertEqual(len(unified.edges), 1)

    def test_query_lineage(self):
        stitcher = LineageStitcher()
        node = LineageNode("test:table1", "test", "table", "table1")
        stitcher.unified_graph.add_node(node)
        result = stitcher.query_lineage({"entity_id": "test:table1"})
        self.assertEqual(result["entity_id"], "test:table1")
        self.assertEqual(result["system"], "test")

    def test_column_lineage(self):
        stitcher = LineageStitcher()
        n1 = LineageNode("s:col1", "s", "column", "col1")
        n2 = LineageNode("s:col2", "s", "column", "col2")
        stitcher.unified_graph.add_node(n1)
        stitcher.unified_graph.add_node(n2)
        stitcher.unified_graph.add_edge(LineageEdge("s:col1", "s:col2"))
        lineage = stitcher.get_column_lineage("s:col2")
        self.assertIn("s:col1", lineage["upstream"])
