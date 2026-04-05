#!/usr/bin/env python3
import argparse
import sys
from scripts.glossary import Term, Ownership, Tag, GlossaryManager


def main():
    parser = argparse.ArgumentParser(description='Manage business glossary')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    add_parser = subparsers.add_parser('add', help='Add a term')
    add_parser.add_argument('--name', required=True)
    add_parser.add_argument('--definition', required=True)
    add_parser.add_argument('--catalog-path', required=True)
    add_parser.add_argument('--owner')
    add_parser.add_argument('--team')
    add_parser.add_argument('--pii', action='store_true')
    add_parser.add_argument('--quality-score', type=float)

    get_parser = subparsers.add_parser('get', help='Get a term')
    get_parser.add_argument('catalog_path')

    list_parser = subparsers.add_parser('list', help='List all terms')

    search_parser = subparsers.add_parser('search', help='Search terms')
    search_parser.add_argument('query')

    delete_parser = subparsers.add_parser('delete', help='Delete a term')
    delete_parser.add_argument('catalog_path')

    args = parser.parse_args()
    manager = GlossaryManager()

    if args.command == 'add':
        ownership = Ownership(args.owner, args.team) if args.owner else None
        term = Term(
            name=args.name,
            definition=args.definition,
            catalog_path=args.catalog_path,
            ownership=ownership,
            pii_status=args.pii,
            quality_score=args.quality_score
        )
        manager.add_term(term)
        print(f"Added term: {args.name}")
    elif args.command == 'get':
        term = manager.get_term(args.catalog_path)
        if term:
            print(f"Name: {term.name}\nDefinition: {term.definition}")
        else:
            print("Term not found")
    elif args.command == 'list':
        for term in manager.list_terms():
            print(f"{term.catalog_path}: {term.name}")
    elif args.command == 'search':
        for term in manager.search_terms(args.query):
            print(f"{term.catalog_path}: {term.name}")
    elif args.command == 'delete':
        if manager.delete_term(args.catalog_path):
            print("Term deleted")
        else:
            print("Term not found")


if __name__ == '__main__':
    main()
