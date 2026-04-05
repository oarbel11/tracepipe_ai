from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class Tag:
    key: str
    value: str


@dataclass
class Ownership:
    owner: str
    team: Optional[str] = None


@dataclass
class Term:
    name: str
    definition: str
    catalog_path: str
    ownership: Optional[Ownership] = None
    tags: List[Tag] = field(default_factory=list)
    pii_status: bool = False
    quality_score: Optional[float] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        return {
            'name': self.name,
            'definition': self.definition,
            'catalog_path': self.catalog_path,
            'ownership': {
                'owner': self.ownership.owner,
                'team': self.ownership.team
            } if self.ownership else None,
            'tags': [{'key': t.key, 'value': t.value} for t in self.tags],
            'pii_status': self.pii_status,
            'quality_score': self.quality_score,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @classmethod
    def from_dict(cls, data):
        ownership = None
        if data.get('ownership'):
            ownership = Ownership(**data['ownership'])
        tags = [Tag(**t) for t in data.get('tags', [])]
        return cls(
            name=data['name'],
            definition=data['definition'],
            catalog_path=data['catalog_path'],
            ownership=ownership,
            tags=tags,
            pii_status=data.get('pii_status', False),
            quality_score=data.get('quality_score'),
            created_at=data.get('created_at', datetime.now().isoformat()),
            updated_at=data.get('updated_at', datetime.now().isoformat())
        )
