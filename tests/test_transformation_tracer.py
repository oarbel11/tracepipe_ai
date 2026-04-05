import pytest
from scripts.transformation_tracer import TransformationTracer, ColumnLineage
from scripts.peer_review import PeerReviewSystem

def test_transformation_tracer_initialization():
    tracer = TransformationTracer()
    assert tracer.graph is not None
    assert tracer.column_lineages == []

def test_parse_simple_sql():
    tracer = TransformationTracer()
    sql = "SELECT id, name FROM users"
    lineages = tracer.parse_sql(sql)
    assert len(lineages) == 2
    assert lineages[0].source_column == "id"
    assert lineages[0].target_column == "id"

def test_parse_sql_with_alias():
    tracer = TransformationTracer()
    sql = "SELECT id AS user_id, name AS full_name FROM users"
    lineages = tracer.parse_sql(sql)
    assert len(lineages) == 2
    assert lineages[0].target_column == "user_id"
    assert lineages[1].target_column == "full_name"

def test_parse_sql_with_transformation():
    tracer = TransformationTracer()
    sql = "SELECT UPPER(name) AS name_upper FROM users"
    lineages = tracer.parse_sql(sql)
    assert len(lineages) == 1
    assert lineages[0].target_column == "name_upper"
    assert "UPPER" in lineages[0].transformation

def test_parse_python_dataframe():
    tracer = TransformationTracer()
    code = 'df["total"] = df["price"] * df["quantity"]'
    lineages = tracer.parse_python(code)
    assert len(lineages) == 2
    assert lineages[0].target_column == "total"
    assert "price" in [l.source_column for l in lineages]

def test_build_graph():
    tracer = TransformationTracer()
    lineages = [
        ColumnLineage("id", "user_id", "direct"),
        ColumnLineage("name", "full_name", "UPPER(name)")
    ]
    tracer.build_graph(lineages)
    assert len(tracer.graph.nodes()) == 4
    assert len(tracer.graph.edges()) == 2

def test_peer_review_analyze_sql():
    system = PeerReviewSystem()
    sql = "SELECT id AS user_id FROM users"
    result = system.analyze_code(sql, "sql")
    assert "lineages" in result
    assert len(result["lineages"]) == 1

def test_peer_review_get_column_lineage():
    system = PeerReviewSystem()
    sql = "SELECT id AS user_id FROM users"
    system.analyze_code(sql, "sql")
    lineage = system.get_column_lineage("user_id")
    assert len(lineage) == 1
    assert lineage[0]["source"] == "id"

def test_peer_review_export_lineage(tmp_path):
    system = PeerReviewSystem()
    sql = "SELECT id AS user_id FROM users"
    system.analyze_code(sql, "sql")
    output_file = tmp_path / "lineage.json"
    system.export_lineage(str(output_file))
    assert output_file.exists()
