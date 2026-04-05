import unittest
import json
from pathlib import Path
from scripts.impact_simulator import ImpactSimulator
from scripts.graph_visualizer import GraphVisualizer
from scripts.blast_radius import BlastRadiusAnalyzer


class TestImpactSimulator(unittest.TestCase):
    def setUp(self):
        self.lineage_data = {
            'nodes': {
                'table1': {'type': 'table', 'name': 'users'},
                'table2': {'type': 'table', 'name': 'orders'},
                'dashboard1': {'type': 'dashboard', 'name': 'Sales Dashboard'},
                'model1': {'type': 'ml_model', 'name': 'Prediction Model'}
            },
            'edges': [
                {'source': 'table1', 'target': 'table2'},
                {'source': 'table2', 'target': 'dashboard1'},
                {'source': 'table2', 'target': 'model1'}
            ]
        }

    def test_impact_simulator_init(self):
        simulator = ImpactSimulator(self.lineage_data)
        self.assertIsNotNone(simulator.graph)

    def test_simulate_change(self):
        simulator = ImpactSimulator(self.lineage_data)
        result = simulator.simulate_change('table1', 'schema_change')
        self.assertEqual(result['change_node'], 'table1')
        self.assertGreater(result['affected_count'], 0)
        self.assertIn('affected_nodes', result)

    def test_find_downstream(self):
        simulator = ImpactSimulator(self.lineage_data)
        downstream = simulator._find_downstream('table1')
        self.assertIsInstance(downstream, list)
        self.assertIn('table2', downstream)

    def test_get_impact_graph(self):
        simulator = ImpactSimulator(self.lineage_data)
        graph = simulator.get_impact_graph('table1')
        self.assertIn('nodes', graph)
        self.assertIn('edges', graph)

    def test_graph_visualizer(self):
        simulator = ImpactSimulator(self.lineage_data)
        graph = simulator.get_impact_graph('table1')
        visualizer = GraphVisualizer(graph)
        ascii_output = visualizer.generate_ascii()
        self.assertIn('Impact Analysis Graph', ascii_output)

    def test_blast_radius_simulation(self):
        temp_file = Path('test_lineage.json')
        with open(temp_file, 'w') as f:
            json.dump(self.lineage_data, f)
        try:
            analyzer = BlastRadiusAnalyzer('test_lineage.json')
            result = analyzer.simulate_scenario('table1', 'deprecation')
            self.assertIn('affected_count', result)
        finally:
            if temp_file.exists():
                temp_file.unlink()


if __name__ == '__main__':
    unittest.main()
