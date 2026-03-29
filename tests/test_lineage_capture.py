import pytest
from scripts.lineage.unmanaged_capture import UnmanagedLineageCapture
from scripts.lineage.udf_mapper import UDFColumnMapper
from scripts.lineage.lineage_orchestrator import LineageOrchestrator


def test_unmanaged_sql_capture():
    capture = UnmanagedLineageCapture()
    sql = """
    CREATE TABLE my_table
    LOCATION 's3://bucket/path/data'
    AS SELECT col1, col2 FROM source_table
    """
    
    result = capture.extract_from_sql(sql)
    assert len(result) > 0
    assert result[0]['type'] == 'unmanaged_table'
    assert 's3://bucket/path/data' in result[0]['location']


def test_python_write_capture():
    capture = UnmanagedLineageCapture()
    code = "df.write.parquet('s3://mybucket/output.parquet')"
    
    result = capture.extract_from_python(code)
    assert len(result) > 0
    assert result[0]['type'] == 'python_write'
    assert 'parquet' in result[0]['location']


def test_udf_python_extraction():
    mapper = UDFColumnMapper()
    code = """
@udf
def transform_data(col1, col2):
    return col1 + col2
    """
    
    result = mapper.extract_udf_lineage(code)
    assert len(result) > 0
    assert result[0]['name'] == 'transform_data'
    assert 'col1' in result[0]['input_columns']
    assert 'col2' in result[0]['input_columns']


def test_udf_sql_extraction():
    mapper = UDFColumnMapper()
    sql = """
    CREATE FUNCTION add_values(x INT, y INT)
    RETURNS INT
    RETURN x + y;
    """
    
    result = mapper.extract_udf_lineage('', sql)
    assert len(result) > 0
    assert result[0]['name'] == 'add_values'
    assert result[0]['return_type'] == 'INT'


def test_orchestrator_analyze_file(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("""
    CREATE TABLE unmanaged_table
    LOCATION 's3://data/output'
    AS SELECT * FROM source
    """)
    
    orchestrator = LineageOrchestrator(str(tmp_path))
    result = orchestrator.analyze_file(str(sql_file))
    
    assert 'unmanaged_writes' in result
    assert len(result['unmanaged_writes']) > 0


def test_orchestrator_scan_repository(tmp_path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("CREATE TABLE t LOCATION 's3://bucket' AS SELECT 1")
    
    py_file = tmp_path / "script.py"
    py_file.write_text("df.write.parquet('output.parquet')")
    
    orchestrator = LineageOrchestrator(str(tmp_path))
    result = orchestrator.scan_repository()
    
    assert result['summary']['total_unmanaged_writes'] >= 2
    assert 'unmanaged_writes' in result
    assert 'udf_mappings' in result
