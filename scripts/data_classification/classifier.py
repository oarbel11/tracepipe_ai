import re
from typing import Dict, List, Set
from dataclasses import dataclass

@dataclass
class Classification:
    asset_id: str
    column_name: str
    classification_type: str
    confidence: float
    policy_tags: List[str]

class SensitiveDataClassifier:
    def __init__(self):
        self.patterns = {
            'email': (r'email|e_mail|mail_address', ['PII', 'contact']),
            'phone': (r'phone|telephone|mobile|cell', ['PII', 'contact']),
            'ssn': (r'ssn|social_security', ['PII', 'sensitive']),
            'credit_card': (r'credit_card|cc_num|card_number', ['PCI', 'sensitive']),
            'name': (r'^name$|first_name|last_name|full_name', ['PII']),
            'dob': (r'birth_date|date_of_birth|dob', ['PII', 'sensitive']),
            'address': (r'address|street|zip|postal', ['PII', 'location']),
            'ip_address': (r'ip_addr|ip_address', ['PII', 'network']),
            'medical': (r'diagnosis|medication|patient|treatment', ['PHI', 'sensitive']),
            'financial': (r'salary|income|account|balance', ['financial', 'sensitive'])
        }

    def classify_column(self, table_name: str, column_name: str) -> List[Classification]:
        results = []
        col_lower = column_name.lower()
        for class_type, (pattern, tags) in self.patterns.items():
            if re.search(pattern, col_lower):
                results.append(Classification(
                    asset_id=f"{table_name}.{column_name}",
                    column_name=column_name,
                    classification_type=class_type,
                    confidence=0.85,
                    policy_tags=tags
                ))
        return results

    def classify_table(self, table_name: str, columns: List[str]) -> List[Classification]:
        classifications = []
        for col in columns:
            classifications.extend(self.classify_column(table_name, col))
        return classifications

    def classify_catalog(self, catalog_name: str, schema_map: Dict[str, List[str]] = None) -> Dict[str, List[Classification]]:
        if schema_map is None:
            schema_map = {
                'customers': ['customer_id', 'email', 'phone', 'name', 'address'],
                'transactions': ['transaction_id', 'amount', 'credit_card']
            }
        result = {}
        for table, columns in schema_map.items():
            full_table = f"{catalog_name}.{table}"
            result[full_table] = self.classify_table(full_table, columns)
        return result

    def get_unique_tags(self, classifications: Dict[str, List[Classification]]) -> Set[str]:
        tags = set()
        for class_list in classifications.values():
            for c in class_list:
                tags.update(c.policy_tags)
        return tags
