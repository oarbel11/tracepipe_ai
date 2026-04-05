from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

@dataclass
class Tag:
    name: str
    category: str = 'general'

@dataclass
class Owner:
    name: str
    email: Optional[str] = None
    team: Optional[str] = None

@dataclass
class Term:
    name: str
    definition: str
    owner: str
    tags: List[str] = field(default_factory=list)
    quality_score: Optional[float] = None
    is_pii: bool = False
    steward: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self):
        return {
            'name': self.name,
            'definition': self.definition,
            'owner': self.owner,
            'tags': ','.join(self.tags),
            'quality_score': self.quality_score,
            'is_pii': self.is_pii,
            'steward': self.steward,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data):
        tags = data.get('tags', '')
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(',') if t.strip()]
        return cls(
            name=data['name'],
            definition=data['definition'],
            owner=data['owner'],
            tags=tags,
            quality_score=data.get('quality_score'),
            is_pii=bool(data.get('is_pii', False)),
            steward=data.get('steward'),
            created_at=data.get('created_at', datetime.utcnow().isoformat()),
            updated_at=data.get('updated_at', datetime.utcnow().isoformat())
        )