"""Tests for Spark lineage parser."""

import pytest
from tracepipe.parsers.spark_lineage import SparkLineageParser


def test_simple_select():
    """Test basic SELECT statement parsing."""
    parser = SparkLineageParser()
    query = "SELECT col1, col2 FROM table1"
    lineage = parser.parse_query(query)
    
    assert "col1" in lineage
    assert "col2" in lineage


def test_select_with_alias():
    """Test SELECT with column aliases."""
    parser = SparkLineageParser()
    query = "SELECT col1 AS alias1, col2 AS alias2 FROM table1"
    lineage = parser.parse_query(query)
    
    assert "alias1" in lineage
    assert "alias2" in lineage
    assert "col1" in lineage["alias1"]
    assert "col2" in lineage["alias2"]


def test_select_with_expression():
    """Test SELECT with expressions."""
    parser = SparkLineageParser()
    query = "SELECT col1 + col2 AS sum_col FROM table1"
    lineage = parser.parse_query(query)
    
    assert "sum_col" in lineage
    assert "col1" in lineage["sum_col"]
    assert "col2" in lineage["sum_col"]
