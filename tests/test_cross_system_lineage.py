import pytest
from scripts.lineage_graph import LineageGraph, LineageNode, NodeType
from scripts.external_lineage_integrator import ExternalLineageIntegrator
from scripts.impact_analyzer import ImpactAnalyzer

def test_lineage_graph_creation():
    graph = LineageGraph()
    assert graph is not None
    assert len(graph.nodes) == 0

def test_add_nodes_and_edges():
    graph = LineageGraph()
    node1 = LineageNode(id="table1", node_type=NodeType.TABLE, system="databricks", metadata={})
    node2 = LineageNode(id="table2", node_type=NodeType.TABLE, system="databricks", metadata={})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("table1", "table2")
    assert len(graph.nodes) == 2
    assert "table2" in graph.get_downstream("table1")
    assert "table1" in graph.get_upstream("table2")

def test_external_lineage_file():
    graph = LineageGraph()
    integrator = ExternalLineageIntegrator(graph)
    integrator.integrate_file_lineage("/data/output.csv", ["table1", "table2"])
    assert "file:///data/output.csv" in graph.nodes
    downstream = graph.get_downstream("table1")
    assert "file:///data/output.csv" in downstream

def test_external_lineage_bi():
    graph = LineageGraph()
    integrator = ExternalLineageIntegrator(graph)
    integrator.integrate_bi_lineage("report_123", ["table1"], "powerbi")
    assert "powerbi://report_123" in graph.nodes
    downstream = graph.get_downstream("table1")
    assert "powerbi://report_123" in downstream

def test_external_lineage_etl():
    graph = LineageGraph()
    integrator = ExternalLineageIntegrator(graph)
    integrator.integrate_etl_lineage("job_456", ["table1"], ["table2"], "airflow")
    assert "airflow://job_456" in graph.nodes
    downstream = graph.get_downstream("table1")
    assert "airflow://job_456" in downstream
    downstream_etl = graph.get_downstream("airflow://job_456")
    assert "table2" in downstream_etl

def test_impact_analyzer_downstream():
    graph = LineageGraph()
    node1 = LineageNode(id="table1", node_type=NodeType.TABLE, system="databricks", metadata={})
    node2 = LineageNode(id="table2", node_type=NodeType.TABLE, system="databricks", metadata={})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("table1", "table2")
    analyzer = ImpactAnalyzer(graph)
    impact = analyzer.analyze_downstream_impact("table1")
    assert impact["impacted_count"] == 1
    assert impact["impacted_nodes"][0]["id"] == "table2"

def test_impact_analyzer_upstream():
    graph = LineageGraph()
    node1 = LineageNode(id="table1", node_type=NodeType.TABLE, system="databricks", metadata={})
    node2 = LineageNode(id="table2", node_type=NodeType.TABLE, system="databricks", metadata={})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("table1", "table2")
    analyzer = ImpactAnalyzer(graph)
    dependencies = analyzer.analyze_upstream_dependencies("table2")
    assert dependencies["dependency_count"] == 1
    assert dependencies["dependency_nodes"][0]["id"] == "table1"

def test_table_rename():
    graph = LineageGraph()
    node1 = LineageNode(id="old_table", node_type=NodeType.TABLE, system="databricks", metadata={})
    graph.add_node(node1)
    analyzer = ImpactAnalyzer(graph)
    analyzer.handle_table_rename("old_table", "new_table")
    assert graph.resolve_id("old_table") == "new_table"
    assert "new_table" in graph.nodes
