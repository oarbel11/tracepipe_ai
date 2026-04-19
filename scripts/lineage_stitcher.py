"""Core lineage stitching engine."""
import re
from typing import Dict, List, Set, Tuple


class LineageStitcher:
    """Stitches lineage from multiple sources."""

    def __init__(self):
        self.stitching_rules = []

    def add_rule(self, pattern: str, source: str, target: str):
        """Add a stitching rule."""
        self.stitching_rules.append({
            'pattern': pattern,
            'source': source,
            'target': target
        })

    def match_entities(self, entity1: Dict, entity2: Dict) -> float:
        """Calculate match score between two entities."""
        score = 0.0
        name1 = entity1.get('name', '').lower()
        name2 = entity2.get('name', '').lower()

        if name1 == name2:
            score += 1.0
        elif name1 in name2 or name2 in name1:
            score += 0.5

        if entity1.get('type') == entity2.get('type'):
            score += 0.3

        return score

    def stitch(self, lineage1: Dict, lineage2: Dict, threshold=0.5) -> List[Tuple]:
        """Stitch two lineage graphs together."""
        mappings = []
        entities1 = lineage1.get('entities', [])
        entities2 = lineage2.get('entities', [])

        for e1 in entities1:
            for e2 in entities2:
                score = self.match_entities(e1, e2)
                if score >= threshold:
                    mappings.append((
                        e1.get('id'),
                        e2.get('id'),
                        score
                    ))

        return mappings

    def merge_lineage(self, lineages: List[Dict]) -> Dict:
        """Merge multiple lineage graphs."""
        merged = {'entities': [], 'edges': []}
        entity_map = {}

        for lineage in lineages:
            for entity in lineage.get('entities', []):
                eid = entity.get('id')
                if eid not in entity_map:
                    entity_map[eid] = entity
                    merged['entities'].append(entity)

            for edge in lineage.get('edges', []):
                if edge not in merged['edges']:
                    merged['edges'].append(edge)

        return merged
