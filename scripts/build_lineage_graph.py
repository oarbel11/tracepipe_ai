#!/usr/bin/env python3
import sys
from lineage_integration import LineageEngine
import json

def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'config/config.yml'
    
    engine = LineageEngine(config_path)
    
    print("Building unified lineage graph...")
    graph = engine.build_unified_lineage()
    
    print(f"\nLineage Graph Summary:")
    print(f"  Total nodes: {graph.number_of_nodes()}")
    print(f"  Total edges: {graph.number_of_edges()}")
    print(f"  Column-level edges: {engine.column_graph.number_of_edges()}")
    
    if graph.number_of_nodes() > 0:
        print("\nSample lineage paths:")
        for node in list(graph.nodes())[:3]:
            downstream = engine.query_lineage(node, 'downstream')
            print(f"  {node} -> {len(downstream['dependencies'])} downstream dependencies")
    
    output_file = 'lineage_graph.json'
    lineage_data = {
        'nodes': list(graph.nodes()),
        'edges': [(u, v, graph[u][v]) for u, v in graph.edges()]
    }
    with open(output_file, 'w') as f:
        json.dump(lineage_data, f, indent=2)
    print(f"\nLineage graph saved to {output_file}")

if __name__ == '__main__':
    main()
