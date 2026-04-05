import argparse
import json
import sys
from .impact_simulator import ImpactSimulator
from .peer_review.peer_review import PeerReviewOrchestrator


def main():
    parser = argparse.ArgumentParser(description='TracePipe AI CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    sim_parser = subparsers.add_parser('simulate-impact', 
                                       help='Simulate impact of changes')
    sim_parser.add_argument('--target', required=True, 
                           help='Target asset (e.g., catalog.schema.table)')
    sim_parser.add_argument('--change-type', required=True,
                           choices=['drop_column', 'schema_change', 
                                   'deprecate', 'data_quality'],
                           help='Type of change to simulate')
    sim_parser.add_argument('--change-params', type=str, default='{}',
                           help='JSON string of change parameters')
    sim_parser.add_argument('--output', default='impact_result.json',
                           help='Output file path')
    sim_parser.add_argument('--visualize', action='store_true',
                           help='Generate interactive HTML visualization')
    
    review_parser = subparsers.add_parser('peer-review',
                                         help='Run peer review analysis')
    review_parser.add_argument('--repo-path', required=True,
                              help='Path to repository')
    review_parser.add_argument('--branch', default='main',
                              help='Branch to analyze')
    
    args = parser.parse_args()
    
    if args.command == 'simulate-impact':
        simulator = ImpactSimulator()
        
        try:
            change_params = json.loads(args.change_params)
        except json.JSONDecodeError:
            print('Error: Invalid JSON in --change-params')
            sys.exit(1)
        
        result = simulator.simulate_change(
            target=args.target,
            change_type=args.change_type,
            change_params=change_params
        )
        
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Impact analysis saved to {args.output}")
        print(f"Affected assets: {result['affected_count']}")
        print(f"Risk score: {result['risk_score']:.2f}")
        
        if args.visualize:
            html_file = args.output.replace('.json', '.html')
            simulator.visualize_graph(result, output=html_file)
            print(f"Visualization saved to {html_file}")
    
    elif args.command == 'peer-review':
        orchestrator = PeerReviewOrchestrator()
        result = orchestrator.run_review(args.repo_path, args.branch)
        print(json.dumps(result, indent=2))
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
