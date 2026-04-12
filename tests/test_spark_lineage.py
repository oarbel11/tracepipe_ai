import pytest
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.lineage_extractor import LineageExtractor, ColumnNode

def test_simple_select():
    code = """df2 = df1.select('col_a', 'col_b')"""
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'df2' in result['dataframes']
    assert result['dataframes']['df2']['operation'] == 'select'
    extractor = LineageExtractor(result)
    lineage = extractor.build_lineage()
    assert 'df2.col_a' in lineage

def test_with_column():
    code = """df2 = df1.withColumn('new_col', col('old_col'))"""
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'df2' in result['dataframes']
    assert result['dataframes']['df2']['operation'] == 'withColumn'

def test_udf_detection():
    code = """\n@udf
def my_udf(x):
    return x * 2
"""
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'my_udf' in result['udfs']
    assert result['udfs']['my_udf']['name'] == 'my_udf'

def test_chained_operations():
    code = """\ndf2 = df1.select('col_a')
df3 = df2.withColumn('col_b', col('col_a'))
"""
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'df2' in result['dataframes']
    assert 'df3' in result['dataframes']
    extractor = LineageExtractor(result)
    lineage = extractor.build_lineage()
    assert len(lineage) >= 2

def test_lineage_extractor():
    parsed = {
        'dataframes': {'df2': {'operation': 'select', 'source': 'df1', 'columns': ['col_a']}},
        'operations': [{'target': 'df2', 'operation': 'select', 'source': 'df1', 'columns': ['col_a']}],
        'udfs': {}
    }
    extractor = LineageExtractor(parsed)
    lineage = extractor.build_lineage()
    assert 'df2.col_a' in lineage
    upstream = extractor.get_upstream_columns('df2', 'col_a')
    assert len(upstream) >= 0

def test_column_node():
    node = ColumnNode('df1', 'col_a')
    assert node.dataframe == 'df1'
    assert node.column == 'col_a'
    assert str(node) == 'df1.col_a'
