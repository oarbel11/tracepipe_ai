import pytest
from scripts.column_lineage_tracker import ColumnLineageTracker, UDFAnalyzer
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.advanced_spark_analyzer import AdvancedSparkAnalyzer

def test_udf_analyzer():
    code = '''
def my_udf(price, tax):
    return price + tax
'''
    analyzer = UDFAnalyzer()
    deps = analyzer.analyze(code)
    assert 'my_udf' in deps
    assert 'price' in deps['my_udf']
    assert 'tax' in deps['my_udf']

def test_column_lineage_tracker():
    tracker = ColumnLineageTracker()
    tracker.track_withcolumn('orders', 'total', 'price + tax', ['price', 'tax'])
    flow = tracker.get_column_flow('orders', 'total')
    assert 'price' in flow
    assert 'tax' in flow

def test_spark_lineage_parser():
    parser = SparkLineageParser()
    code = "df2 = df1.withColumn('total', col('price') + col('tax'))"
    parser.parse_notebook(code)
    assert 'df2' in parser.nodes

def test_advanced_spark_analyzer_map():
    analyzer = AdvancedSparkAnalyzer()
    code = "df.map(lambda x: x.price * 2)"
    node = analyzer.analyze_operation(code, 'map', 'node1')
    assert node.operation_type == 'map'
    assert 'x' in node.input_columns

def test_advanced_spark_analyzer_groupby():
    analyzer = AdvancedSparkAnalyzer()
    code = "df.groupBy('category', 'region')"
    node = analyzer.analyze_operation(code, 'groupBy', 'node2')
    assert 'category' in node.input_columns
    assert 'region' in node.input_columns

def test_advanced_spark_analyzer_agg():
    analyzer = AdvancedSparkAnalyzer()
    code = "df.agg(sum('revenue'), avg('profit'))"
    node = analyzer.analyze_operation(code, 'agg', 'node3')
    assert 'revenue' in node.input_columns
    assert 'profit' in node.input_columns

def test_column_lineage_export(tmp_path):
    tracker = ColumnLineageTracker()
    tracker.track_withcolumn('sales', 'revenue', 'price * quantity', ['price', 'quantity'])
    output_file = tmp_path / "lineage.json"
    tracker.export_to_openlineage(str(output_file))
    assert output_file.exists()

def test_trace_column_lineage():
    analyzer = AdvancedSparkAnalyzer()
    analyzer.analyze_operation("df.withColumn('total', col('a') + col('b'))", 'withColumn', 'n1')
    lineage = analyzer.trace_column_lineage('total')
    assert len(lineage) > 0
    assert lineage[0].operation_type == 'withColumn'
