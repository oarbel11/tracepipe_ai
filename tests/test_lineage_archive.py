import pytest
import os
import tempfile
import yaml
from datetime import datetime
from scripts.lineage_archive import LineageArchiver
from scripts.lineage_query import LineageQueryEngine


@pytest.fixture
def test_config():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        config = {
            'lineage_archive_db': ':memory:',
            'databricks': {
                'server_hostname': 'test.databricks.com',
                'http_path': '/sql/1.0/test',
                'access_token': 'test_token'
            }
        }
        yaml.dump(config, f)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def archiver(test_config):
    return LineageArchiver(test_config)


@pytest.fixture
def query_engine(test_config, archiver):
    mock_data = [
        {'catalog': 'main', 'schema': 'sales', 'table': 'orders',
         'upstream': ['main.raw.raw_orders'], 'downstream': ['main.analytics.order_summary'],
         'updated': datetime.now()}
    ]
    archiver.archive_lineage(mock_data)
    return LineageQueryEngine(test_config)


def test_schema_initialization(archiver):
    result = archiver.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    table_names = [r[0] for r in result]
    assert 'lineage_snapshots' in table_names
    assert 'lineage_edges' in table_names


def test_archive_lineage(archiver):
    mock_data = [
        {'catalog': 'main', 'schema': 'sales', 'table': 'orders',
         'upstream': ['main.raw.raw_orders'], 'downstream': [],
         'updated': datetime.now()}
    ]
    snapshot_id = archiver.archive_lineage(mock_data)
    assert snapshot_id.startswith('snap_')
    result = archiver.conn.execute(
        "SELECT COUNT(*) FROM lineage_snapshots WHERE snapshot_id = ?",
        [snapshot_id]
    ).fetchone()
    assert result[0] == 1


def test_query_entity_lineage(query_engine):
    results = query_engine.query_entity_lineage('main.sales.orders')
    assert len(results) > 0
    assert results[0]['source'] == 'main.raw.raw_orders'


def test_audit_report(query_engine):
    report = query_engine.audit_report('main.sales.orders')
    assert 'entity' in report
    assert report['entity'] == 'main.sales.orders'
    assert report['total_edges'] > 0


def test_query_snapshots(query_engine):
    snapshots = query_engine.query_snapshots()
    assert len(snapshots) > 0
    assert 'id' in snapshots[0]
    assert 'timestamp' in snapshots[0]
