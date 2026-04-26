import pytest
from scripts.lineage_graph import LineageGraph, LineageNode
from scripts.external_lineage_integrator import ExternalLineageIntegrator
from scripts.impact_analyzer import ImpactAnalyzer

def test_lineage_graph_basic():
    graph = LineageGraph()
    node1 = LineageNode("table1", "table", "databricks")
    node2 = LineageNode("table2", "table", "databricks")
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("table1", "table2", {"operation": "transform"})
    assert graph.graph.has_node("table1")
    assert graph.graph.has_edge("table1", "table2")

def test_table_rename():
    graph = LineageGraph()
    graph.add_node(LineageNode("old_table", "table", "databricks"))
    graph.add_node(LineageNode("source", "table", "databricks"))
    graph.add_edge("source", "old_table")
    graph.register_rename("old_table", "new_table")
    resolved = graph.resolve_node("old_table")
    assert resolved == "new_table"
    assert graph.graph.has_edge("source", "new_table")

def test_upstream_downstream():
    graph = LineageGraph()
    for i in range(5):
        graph.add_node(LineageNode(f"node{i}", "table", "test"))
    graph.add_edge("node0", "node1")
    graph.add_edge("node1", "node2")
    graph.add_edge("node2", "node3")
    graph.add_edge("node3", "node4")
    upstream = graph.get_upstream("node3")
    downstream = graph.get_downstream("node1")
    assert len(upstream) == 3
    assert len(downstream) == 3

def test_external_lineage_file():
    graph = LineageGraph()
    integrator = ExternalLineageIntegrator(graph)
    integrator.ingest_file_lineage("s3://bucket/data.csv", "catalog.schema.table", "s3")
    assert graph.graph.has_node("s3://bucket/data.csv")
    assert graph.graph.has_edge("s3://bucket/data.csv", "catalog.schema.table")

def test_bi_lineage():
    graph = LineageGraph()
    integrator = ExternalLineageIntegrator(graph)
    integrator.ingest_bi_lineage("dashboard_123", ["table1", "table2"], "tableau")
    assert graph.graph.has_node("dashboard_123")
    assert graph.graph.has_edge("table1", "dashboard_123")

def test_impact_analysis():
    graph = LineageGraph()
    analyzer = ImpactAnalyzer(graph)
    graph.add_node(LineageNode("source", "table", "databricks"))
    graph.add_node(LineageNode("target", "table", "databricks"))
    graph.add_edge("source", "target")
    impact = analyzer.analyze_impact("source")
    assert impact["exists"] is True
    assert impact["downstream_count"] == 1
    assert impact["upstream_count"] == 0

def test_critical_downstream():
    graph = LineageGraph()
    analyzer = ImpactAnalyzer(graph)
    graph.add_node(LineageNode("source", "table", "databricks"))
    graph.add_node(LineageNode("dashboard", "dashboard", "powerbi"))
    graph.add_edge("source", "dashboard")
    impact = analyzer.analyze_impact("source")
    assert "dashboard" in impact["critical_downstream"]
