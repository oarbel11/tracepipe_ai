import pytest
from scripts.lineage.column_lineage_extractor import ColumnLineageExtractor
from scripts.lineage.column_impact_analyzer import ColumnImpactAnalyzer
from scripts.lineage.lineage_visualizer import LineageVisualizer


def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = """
    SELECT 
        customer_id,
        SUM(amount) AS total_amount,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
    """
    result = extractor.extract_from_sql(sql, 'customer_summary')
    
    assert result['table'] == 'customer_summary'
    assert 'customer_id' in result['columns'] or 'total_amount' in result['columns']


def test_transformation_detection():
    extractor = ColumnLineageExtractor()
    sql = "SELECT revenue = price * quantity, status = CASE WHEN active THEN 1 ELSE 0 END"
    
    assert extractor._detect_transformation_type(sql, 'revenue') == 'arithmetic'
    assert extractor._detect_transformation_type(sql, 'status') == 'conditional'


def test_column_impact_analyzer():
    analyzer = ColumnImpactAnalyzer()
    
    lineage_data = [
        {
            'table': 'source',
            'columns': ['col_a'],
            'dependencies': {'col_a': []},
            'transformations': {'col_a': {'type': 'direct', 'expression': 'col_a', 'udfs': []}}
        },
        {
            'table': 'target',
            'columns': ['col_b'],
            'dependencies': {'col_b': ['source.col_a']},
            'transformations': {'col_b': {'type': 'function', 'expression': 'UPPER(col_a)', 'udfs': []}}
        }
    ]
    
    analyzer.build_graph(lineage_data)
    impact = analyzer.analyze_impact('source.col_a')
    
    assert impact['column'] == 'source.col_a'
    assert 'target.col_b' in impact['downstream_columns']
    assert impact['impact_count'] >= 1
    assert 'criticality' in impact


def test_lineage_visualizer():
    analyzer = ColumnImpactAnalyzer()
    lineage_data = [
        {
            'table': 'raw',
            'columns': ['id'],
            'dependencies': {'id': []},
            'transformations': {'id': {'type': 'direct', 'expression': 'id', 'udfs': []}}
        }
    ]
    analyzer.build_graph(lineage_data)
    
    visualizer = LineageVisualizer(analyzer)
    view = visualizer.generate_interactive_view('raw.id')
    
    assert view['type'] == 'interactive_lineage'
    assert view['root_column'] == 'raw.id'
    assert 'graph' in view
    assert 'nodes' in view['graph']


def test_impact_summary():
    analyzer = ColumnImpactAnalyzer()
    lineage_data = [
        {
            'table': 't1',
            'columns': ['c1'],
            'dependencies': {'c1': []},
            'transformations': {'c1': {'type': 'direct', 'expression': '', 'udfs': []}}
        }
    ]
    analyzer.build_graph(lineage_data)
    
    summary = analyzer.get_impact_summary()
    assert isinstance(summary, dict)


def test_markdown_export():
    analyzer = ColumnImpactAnalyzer()
    lineage_data = [
        {
            'table': 'test',
            'columns': ['col'],
            'dependencies': {'col': []},
            'transformations': {'col': {'type': 'direct', 'expression': 'col', 'udfs': []}}
        }
    ]
    analyzer.build_graph(lineage_data)
    
    visualizer = LineageVisualizer(analyzer)
    md = visualizer.export_markdown('test.col')
    
    assert 'Column Lineage' in md
    assert 'test.col' in md
