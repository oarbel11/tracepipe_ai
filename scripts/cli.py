import argparse
import sys
import json
from scripts.business_glossary import GlossaryManager
from scripts.semantic_lineage import SemanticLineageBuilder

def glossary_commands(args):
    glossary = GlossaryManager()

    if args.glossary_action == 'add-term':
        term_id = glossary.add_term(
            term_name=args.term,
            definition=args.definition,
            owner=args.owner,
            category=args.category
        )
        print(f"Added term: {term_id}")

    elif args.glossary_action == 'link':
        glossary.link_term_to_asset(
            term_id=args.term.lower().replace(' ', '_'),
            asset_path=args.asset,
            asset_type=args.asset_type or 'table'
        )
        print(f"Linked {args.term} to {args.asset}")

    elif args.glossary_action == 'show-terms':
        terms = glossary.get_terms_for_asset(args.asset)
        print(json.dumps(terms, indent=2))

    elif args.glossary_action == 'lineage':
        builder = SemanticLineageBuilder(glossary)
        lineage = builder.get_semantic_lineage(args.asset, depth=args.depth or 3)
        print(json.dumps(lineage, indent=2))

def main():
    parser = argparse.ArgumentParser(description='Tracepipe AI CLI')
    subparsers = parser.add_subparsers(dest='command')

    glossary_parser = subparsers.add_parser('glossary', help='Business glossary management')
    glossary_parser.add_argument('glossary_action',
                                 choices=['add-term', 'link', 'show-terms', 'lineage'])
    glossary_parser.add_argument('--term', help='Business term name')
    glossary_parser.add_argument('--definition', help='Term definition')
    glossary_parser.add_argument('--owner', help='Term owner')
    glossary_parser.add_argument('--category', help='Term category')
    glossary_parser.add_argument('--asset', help='Asset path (catalog.schema.table)')
    glossary_parser.add_argument('--asset-type', help='Asset type (table, column, etc.)')
    glossary_parser.add_argument('--depth', type=int, help='Lineage depth')

    args = parser.parse_args()

    if args.command == 'glossary':
        glossary_commands(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
