import pytest
import ast
from scripts.column_lineage import ColumnLineageAnalyzer, ColumnNode
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.runtime_lineage import SparkPlanAnalyzer
import tempfile
import os

@pytest.fixture
def sample_notebook():
    code = '''
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col

@udf
def calculate_revenue(price, quantity):
    return price * quantity

df = spark.table("sales")
df2 = df.withColumn("revenue", calculate_revenue(col("price"), col("quantity")))
df3 = df2.select("customer_id", "revenue")
df3.write.saveAsTable("revenue_summary")
'''
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(code)
        return f.name

def test_column_lineage_analyzer_load(sample_notebook):
    analyzer = ColumnLineageAnalyzer()
    analyzer.load_notebook(sample_notebook)
    assert analyzer.current_notebook is not None
    os.unlink(sample_notebook)

def test_udf_extraction(sample_notebook):
    analyzer = ColumnLineageAnalyzer()
    analyzer.load_notebook(sample_notebook)
    lineage = analyzer.analyze()
    assert 'calculate_revenue' in lineage.udf_definitions
    os.unlink(sample_notebook)

def test_dataframe_operations(sample_notebook):
    analyzer = ColumnLineageAnalyzer()
    analyzer.load_notebook(sample_notebook)
    lineage = analyzer.analyze()
    assert len(lineage.dataframe_ops) > 0
    with_col_ops = [op for op in lineage.dataframe_ops if op['op'] == 'withColumn']
    assert len(with_col_ops) > 0
    os.unlink(sample_notebook)

def test_spark_lineage_parser(sample_notebook):
    parser = SparkLineageParser()
    result = parser.parse_notebook(sample_notebook)
    assert result['type'] == 'notebook'
    assert 'calculate_revenue' in result['udfs']
    assert result['operations'] > 0
    os.unlink(sample_notebook)

def test_column_dependencies(sample_notebook):
    parser = SparkLineageParser()
    parser.parse_notebook(sample_notebook)
    graph = parser.build_lineage_graph()
    assert graph.number_of_nodes() > 0
    os.unlink(sample_notebook)

def test_runtime_plan_analyzer():
    plan_json = '''{
        "nodeName": "Project",
        "output": [{"name": "col1"}, {"name": "col2"}]
    }'''
    analyzer = SparkPlanAnalyzer()
    result = analyzer.parse_execution_plan(plan_json)
    assert 'transformations' in result
    assert len(result['transformations']) > 0

def test_runtime_capture():
    analyzer = SparkPlanAnalyzer()
    capture = analyzer.capture_runtime_lineage(
        'exec_123', 'test_job', '{"nodeName": "Project"}'
    )
    assert capture.execution_id == 'exec_123'
    assert capture.job_name == 'test_job'
    retrieved = analyzer.get_column_flow('exec_123')
    assert retrieved is not None
    assert retrieved.execution_id == 'exec_123'
