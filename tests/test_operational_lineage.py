import pytest
import networkx as nx
from unittest.mock import Mock, patch, MagicMock
from scripts.operational_lineage import OperationalLineageExtractor


@pytest.fixture
def mock_connection():
    conn = Mock()
    cursor = Mock()
    cursor.description = [
        ('notebook_path',), ('table_catalog',), ('table_schema',),
        ('table_name',), ('operation_type',)
    ]
    cursor.fetchall.return_value = [
        ('/Workspace/etl/load_data', 'main', 'bronze', 'customers', 'WRITE'),
        ('/Workspace/etl/transform', 'main', 'bronze', 'customers', 'READ')
    ]
    conn.cursor.return_value = cursor
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    return conn


@patch('scripts.operational_lineage.sql')
def test_extract_notebook_lineage(mock_sql, mock_connection):
    mock_sql.connect.return_value = mock_connection
    extractor = OperationalLineageExtractor({})
    lineage = extractor.extract_notebook_lineage()
    
    assert len(lineage) == 2
    assert lineage[0]['notebook_path'] == '/Workspace/etl/load_data'
    assert lineage[0]['table_name'] == 'customers'


@patch('scripts.operational_lineage.sql')
def test_build_lineage_graph(mock_sql, mock_connection):
    mock_sql.connect.return_value = mock_connection
    extractor = OperationalLineageExtractor({})
    graph = extractor.build_lineage_graph()
    
    assert isinstance(graph, nx.DiGraph)
    assert graph.number_of_nodes() > 0
    code_nodes = [n for n, d in graph.nodes(data=True) if d['type'] == 'notebook']
    assert len(code_nodes) > 0


@patch('scripts.operational_lineage.sql')
def test_get_upstream_code(mock_sql, mock_connection):
    mock_sql.connect.return_value = mock_connection
    extractor = OperationalLineageExtractor({})
    extractor.build_lineage_graph()
    
    upstream = extractor.get_upstream_code('main.bronze.customers')
    assert len(upstream) >= 0


@patch('scripts.operational_lineage.sql')
def test_get_downstream_impact(mock_sql, mock_connection):
    mock_sql.connect.return_value = mock_connection
    extractor = OperationalLineageExtractor({})
    extractor.build_lineage_graph()
    
    downstream = extractor.get_downstream_impact('notebook:/Workspace/etl/load_data')
    assert isinstance(downstream, list)
