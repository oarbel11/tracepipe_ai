import pytest
from scripts.transformation_tracer import TransformationTracer, ColumnLineage
from scripts.transformation_visualizer import TransformationVisualizer

def test_column_lineage_basic():
    lineage = ColumnLineage()
    lineage.add_column('sales', 'price')
    lineage.add_column('sales', 'quantity')
    lineage.add_transformation(['sales.price', 'sales.quantity'],
                              'sales.total', 'arithmetic',
                              'price * quantity')
    
    result = lineage.get_column_lineage('sales.total')
    assert 'sales.price' in result['depends_on']
    assert 'sales.quantity' in result['depends_on']
    assert result['transformation']['operation'] == 'arithmetic'

def test_trace_simple_select():
    tracer = TransformationTracer()
    sql = "SELECT customer_id, price * 1.1 AS final_price FROM sales"
    lineage = tracer.trace_transformations(sql, source_table='sales')
    
    assert lineage.graph.number_of_nodes() > 0
    assert 'sales_derived.final_price' in lineage.transformations

def test_trace_aggregation():
    tracer = TransformationTracer()
    sql = "SELECT region, SUM(revenue) AS total_revenue FROM sales"
    lineage = tracer.trace_transformations(sql, source_table='sales')
    
    transform = lineage.transformations.get('sales_derived.total_revenue')
    assert transform is not None
    assert transform['operation'] == 'aggregation'

def test_visualizer_to_json():
    lineage = ColumnLineage()
    lineage.add_column('sales', 'price')
    lineage.add_transformation(['sales.price'], 'sales.discounted_price',
                              'arithmetic', 'price * 0.9')
    
    visualizer = TransformationVisualizer(lineage.graph)
    json_output = visualizer.to_json()
    
    assert 'nodes' in json_output
    assert 'edges' in json_output
    assert 'sales.price' in json_output

def test_visualizer_mermaid():
    lineage = ColumnLineage()
    lineage.add_column('sales', 'price')
    lineage.add_transformation(['sales.price'], 'derived.final_price',
                              'arithmetic', 'price * 1.1')
    
    visualizer = TransformationVisualizer(lineage.graph)
    mermaid = visualizer.to_mermaid()
    
    assert 'graph TD' in mermaid
    assert '-->' in mermaid

def test_transformation_summary():
    lineage = ColumnLineage()
    lineage.add_column('sales', 'price')
    lineage.add_column('sales', 'qty')
    lineage.add_transformation(['sales.price', 'sales.qty'],
                              'sales.total', 'arithmetic', 'price * qty')
    
    visualizer = TransformationVisualizer(lineage.graph)
    summary = visualizer.get_transformation_summary()
    
    assert summary['total_columns'] == 3
    assert summary['total_transformations'] == 2
    assert 'arithmetic' in summary['operations']

def test_complex_transformations():
    lineage = ColumnLineage()
    lineage.add_column('sales', 'a')
    lineage.add_column('sales', 'b')
    lineage.add_column('sales', 'c')
    lineage.add_transformation(['sales.a', 'sales.b', 'sales.c'],
                              'sales.complex', 'arithmetic', 'a+b+c')
    
    visualizer = TransformationVisualizer(lineage.graph)
    complex_cols = visualizer.find_complex_transformations(threshold=3)
    
    assert len(complex_cols) == 1
    assert complex_cols[0]['column'] == 'sales.complex'
    assert complex_cols[0]['depends_on_count'] == 3
