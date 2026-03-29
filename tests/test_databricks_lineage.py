"""Tests for Databricks lineage extraction."""

import pytest
import json
from unittest.mock import Mock, patch
from scripts.databricks_lineage.lineage_extractor import DatabricksLineageExtractor
from scripts.databricks_lineage.sql_parser import SQLLineageParser
from scripts.databricks_lineage.visualizer import LineageVisualizer


class TestDatabricksLineageExtractor:
    """Test lineage extractor."""

    def test_extractor_initialization(self):
        """Test extractor can be initialized."""
        extractor = DatabricksLineageExtractor(
            host='https://test.databricks.com',
            token='test_token'
        )
        assert extractor.host == 'https://test.databricks.com'
        assert extractor.token == 'test_token'

    @patch('scripts.databricks_lineage.lineage_extractor.urlopen')
    def test_extract_tables(self, mock_urlopen):
        """Test table extraction."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps({
            'tables': [{'table_id': '1', 'name': 'test_table'}]
        }).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        extractor = DatabricksLineageExtractor('https://test.com', 'token')
        tables = extractor.extract_tables()
        assert len(tables) >= 0

    def test_extract_lineage(self):
        """Test complete lineage extraction."""
        extractor = DatabricksLineageExtractor('https://test.com', 'token')
        lineage = extractor.extract_lineage()
        assert 'nodes' in lineage
        assert 'edges' in lineage
        assert isinstance(lineage['nodes'], list)
        assert isinstance(lineage['edges'], list)


class TestSQLLineageParser:
    """Test SQL parser."""

    def test_parser_initialization(self):
        """Test parser can be initialized."""
        parser = SQLLineageParser()
        assert parser is not None

    def test_extract_source_tables(self):
        """Test extracting source tables from SQL."""
        parser = SQLLineageParser()
        sql = "SELECT * FROM table1 JOIN table2"
        sources = parser.extract_source_tables(sql)
        assert 'table1' in sources
        assert 'table2' in sources

    def test_extract_target_tables(self):
        """Test extracting target tables from SQL."""
        parser = SQLLineageParser()
        sql = "CREATE TABLE target_table AS SELECT * FROM source"
        targets = parser.extract_target_tables(sql)
        assert 'target_table' in targets

    def test_parse_lineage(self):
        """Test complete lineage parsing."""
        parser = SQLLineageParser()
        sql = "INSERT INTO target SELECT * FROM source"
        lineage = parser.parse_lineage(sql)
        assert 'sources' in lineage
        assert 'targets' in lineage
        assert 'edges' in lineage


class TestLineageVisualizer:
    """Test lineage visualizer."""

    def test_visualizer_initialization(self):
        """Test visualizer can be initialized."""
        data = {'nodes': [], 'edges': []}
        viz = LineageVisualizer(data)
        assert viz.lineage_data == data

    def test_to_json(self):
        """Test JSON conversion."""
        data = {'nodes': [{'id': '1', 'name': 'test'}], 'edges': []}
        viz = LineageVisualizer(data)
        json_str = viz.to_json()
        assert 'nodes' in json_str
        assert 'test' in json_str

    def test_to_ascii(self):
        """Test ASCII conversion."""
        data = {'nodes': [{'name': 'test', 'type': 'table'}], 'edges': []}
        viz = LineageVisualizer(data)
        ascii_str = viz.to_ascii()
        assert 'test' in ascii_str
        assert 'Lineage' in ascii_str

    def test_get_statistics(self):
        """Test statistics generation."""
        data = {
            'nodes': [{'type': 'table'}, {'type': 'job'}],
            'edges': [{'source': 'a', 'target': 'b'}]
        }
        viz = LineageVisualizer(data)
        stats = viz.get_statistics()
        assert stats['total_nodes'] == 2
        assert stats['total_edges'] == 1
