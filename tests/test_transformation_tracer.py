import pytest
from scripts.transformation_tracer import TransformationTracer, ColumnLineage

def test_parse_sql_basic():
    tracer = TransformationTracer()
    sql = "SELECT id, name FROM users"
    lineages = tracer.parse_sql(sql, source_table="users", target_table="output")
    assert len(lineages) == 2
    assert lineages[0].source_column == "id"
    assert lineages[0].target_column == "id"
    assert lineages[0].transformation == "direct"

def test_parse_sql_with_alias():
    tracer = TransformationTracer()
    sql = "SELECT user_id AS id, UPPER(name) AS full_name FROM users"
    lineages = tracer.parse_sql(sql, source_table="users")
    assert len(lineages) == 2
    assert lineages[0].target_column == "id"
    assert lineages[1].target_column == "full_name"
    assert "UPPER(name)" in lineages[1].transformation

def test_parse_python_dataframe():
    tracer = TransformationTracer()
    code = """df['total'] = df['price'] * df['quantity']
df['name_upper'] = df['name'].upper()"""
    lineages = tracer.parse_python(code)
    assert len(lineages) == 2
    assert lineages[0].target_column == "total"
    assert "price" in lineages[0].source_column

def test_build_graph():
    tracer = TransformationTracer()
    tracer.parse_sql("SELECT id, name AS full_name FROM users", "users", "output")
    graph = tracer.build_graph()
    assert 'nodes' in graph
    assert 'edges' in graph
    assert len(graph['nodes']) > 0
    assert len(graph['edges']) == 2

def test_get_lineage_for_column():
    tracer = TransformationTracer()
    tracer.parse_sql("SELECT id, name AS full_name FROM users")
    lineages = tracer.get_lineage_for_column("full_name")
    assert len(lineages) == 1
    assert lineages[0].target_column == "full_name"

def test_export_json():
    tracer = TransformationTracer()
    tracer.parse_sql("SELECT id FROM users")
    json_output = tracer.export_json()
    assert "id" in json_output
    assert "source_column" in json_output
