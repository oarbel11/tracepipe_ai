import pytest
from scripts.lineage_extractor import LineageExtractor, ColumnNode

def test_lineage_extractor_init():
    extractor = LineageExtractor()
    assert extractor.graph is not None
    assert extractor.integrator is not None
    assert extractor.analyzer is not None

def test_extract_lineage():
    extractor = LineageExtractor()
    result = extractor.extract_lineage("catalog.schema.table")
    assert result['table'] == "catalog.schema.table"
    assert 'upstream' in result
    assert 'downstream' in result

def test_column_lineage():
    extractor = LineageExtractor()
    col = ColumnNode("catalog", "schema", "table", "column")
    lineage = extractor.get_column_lineage(col)
    assert isinstance(lineage, list)

def test_external_etl_integration():
    extractor = LineageExtractor()
    lineage_data = [
        {'source_id': 's3://bucket/data', 'target_id': 'table:target',
         'source_name': 'source', 'target_name': 'target'}
    ]
    extractor.integrate_etl_lineage('airflow', lineage_data)
    impact = extractor.analyze_impact('s3://bucket/data')
    assert impact['impacted_count'] >= 0

def test_bi_integration():
    extractor = LineageExtractor()
    lineage_data = [
        {'source_id': 'table:sales', 'report_id': 'report:dashboard',
         'report_name': 'Sales Dashboard'}
    ]
    extractor.integrate_bi_lineage('tableau', lineage_data)
    deps = extractor.analyze_dependencies('report:dashboard')
    assert deps['dependency_count'] >= 0

def test_file_lineage():
    extractor = LineageExtractor()
    extractor.integrate_file_lineage('/path/to/file.csv', 'table:data', 
                                    'read')
    impact = extractor.analyze_impact('file:/path/to/file.csv')
    assert 'impacted_count' in impact

def test_table_rename():
    extractor = LineageExtractor()
    extractor.extract_lineage('old_table')
    extractor.handle_table_rename('old_table', 'new_table')
    assert extractor.graph.get_node('table:new_table') is not None

def test_impact_analysis():
    extractor = LineageExtractor()
    extractor.extract_lineage('source_table')
    impact = extractor.analyze_impact('table:source_table')
    assert 'impacted_count' in impact
    assert 'impacted_nodes' in impact

def test_dependency_analysis():
    extractor = LineageExtractor()
    extractor.extract_lineage('target_table')
    deps = extractor.analyze_dependencies('table:target_table')
    assert 'dependency_count' in deps
    assert 'dependency_nodes' in deps
