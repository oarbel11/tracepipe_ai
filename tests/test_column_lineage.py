import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.lineage.lineage_parser import ColumnLineageParser, ColumnLineage
from scripts.lineage.lineage_tracker import LineageTracker
from scripts.lineage.udf_analyzer import UDFAnalyzer

def test_simple_select_lineage():
    parser = ColumnLineageParser()
    sql = "SELECT a.customer_id, a.name FROM customers a"
    lineages = parser.parse_sql(sql)
    assert len(lineages) == 2
    assert lineages[0].target_column == "customer_id"
    assert lineages[1].target_column == "name"

def test_complex_transformation_lineage():
    parser = ColumnLineageParser()
    sql = "SELECT UPPER(c.first_name) AS name, c.age + 1 AS next_age FROM clients c"
    lineages = parser.parse_sql(sql)
    assert len(lineages) == 2
    assert any('first_name' in str(l.source_columns) for l in lineages)

def test_lineage_tracker_storage(tmp_path):
    storage = tmp_path / "test_lineage.json"
    tracker = LineageTracker(str(storage))
    sql = "SELECT id, name FROM users"
    tracker.track_sql_lineage(sql, "target_table")
    assert storage.exists()
    assert "target_table" in tracker.lineage_data["columns"]

def test_manual_lineage_override(tmp_path):
    storage = tmp_path / "test_manual.json"
    tracker = LineageTracker(str(storage))
    tracker.add_manual_lineage(
        "orders", "total_price",
        [{"table": "line_items", "column": "price"}]
    )
    lineage = tracker.get_column_lineage("orders", "total_price")
    assert lineage["manual"] is True

def test_udf_analyzer_python():
    analyzer = UDFAnalyzer()
    code = "def process(x, y):\n    return x + y"
    result = analyzer.analyze_udf(code)
    assert "x" in result["inputs"]
    assert "y" in result["inputs"]

def test_path_based_source_tracking(tmp_path):
    storage = tmp_path / "test_path.json"
    tracker = LineageTracker(str(storage))
    tracker.track_path_based_source(
        "/mnt/data/sales.parquet",
        "sales_table",
        {"id": "int", "amount": "float"}
    )
    assert "sales_table" in tracker.lineage_data["tables"]
    assert tracker.lineage_data["tables"]["sales_table"]["path"] == "/mnt/data/sales.parquet"

def test_impact_analysis(tmp_path):
    storage = tmp_path / "test_impact.json"
    tracker = LineageTracker(str(storage))
    sql = "SELECT u.user_id, u.email FROM users u"
    tracker.track_sql_lineage(sql, "user_profile")
    impact = tracker.get_impact_analysis("users", "user_id")
    assert "user_profile.user_id" in impact
