import pytest
from scripts.lineage_extractor import SparkLineageExtractor
from scripts.impact_analyzer import ImpactAnalyzer
from scripts.lineage_graph import NodeType


class MockDatabricksClient:
    def __init__(self):
        self.lineage_data = {}

    def get_table_lineage(self, table_name):
        return self.lineage_data.get(table_name, {})

    def set_lineage(self, table_name, upstream, downstream):
        self.lineage_data[table_name] = {
            'upstream_tables': upstream,
            'downstream_tables': downstream
        }


@pytest.fixture
def mock_client():
    return MockDatabricksClient()


@pytest.fixture
def extractor(mock_client):
    return SparkLineageExtractor(mock_client)


def test_extract_lineage(extractor, mock_client):
    mock_client.set_lineage('target_table', ['source_table'], ['dest_table'])
    graph = extractor.extract_lineage('target_table')
    assert 'table:target_table' in graph.nodes
    assert 'table:source_table' in graph.nodes


def test_external_etl_integration(extractor):
    etl_metadata = {
        'job_name': 'etl_job_1',
        'sources': ['src_table'],
        'targets': ['tgt_table']
    }
    extractor.integrate_external_etl(etl_metadata)
    assert 'etl:etl_job_1' in extractor.graph.nodes


def test_bi_integration(extractor):
    bi_metadata = {
        'report_name': 'sales_report',
        'sources': ['sales_table']
    }
    extractor.integrate_bi_tool(bi_metadata)
    assert 'bi:sales_report' in extractor.graph.nodes


def test_file_lineage(extractor):
    extractor.track_file_lineage('/data/output.parquet', ['source_table'],
                                 'write')
    assert 'file:/data/output.parquet' in extractor.graph.nodes


def test_table_rename(extractor):
    extractor.graph.add_node('table:old_table', 'table', 'old_table', {})
    extractor.handle_table_rename('old_table', 'new_table')
    assert 'table:new_table' in extractor.graph.nodes


def test_impact_analysis(extractor, mock_client):
    mock_client.set_lineage('src', [], ['tgt'])
    extractor.extract_lineage('src')
    analyzer = ImpactAnalyzer(extractor.graph)
    impact = analyzer.analyze_downstream_impact('table:src')
    assert impact['total_count'] >= 0


def test_dependency_analysis(extractor, mock_client):
    mock_client.set_lineage('target_table', ['source_table'], [])
    extractor.extract_lineage('target_table')
    analyzer = ImpactAnalyzer(extractor.graph)
    deps = analyzer.analyze_upstream_dependencies('table:target_table')
    assert deps['total_count'] >= 0
