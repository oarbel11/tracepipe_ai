"""Basic import test to verify package structure."""

import sys
from pathlib import Path

# Add tracepipe_ai to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tracepipe.parsers.spark_lineage import SparkLineageParser


def test_import():
    """Test that SparkLineageParser can be imported."""
    parser = SparkLineageParser()
    assert parser is not None


def test_basic_parsing():
    """Test basic parsing functionality."""
    parser = SparkLineageParser()
    code = '''
df1 = spark.read.table("source")
df2 = df1.select("col1", "col2")
'''
    result = parser.parse_code(code)
    assert isinstance(result, dict)
    assert "df2" in result
    assert "col1" in result["df2"]
    assert result["df2"]["col1"] == ["df1.col1"]
