import re
from enum import Enum
from typing import List, Dict, Optional
from dataclasses import dataclass

class SensitivityLevel(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

@dataclass
class Classification:
    column_name: str
    data_type: str
    sensitivity_level: SensitivityLevel
    detected_patterns: List[str]
    confidence: float

class SensitiveDataClassifier:
    def __init__(self):
        self.patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'
        }
        self.sensitive_keywords = {
            'ssn', 'social_security', 'email', 'phone', 'credit_card',
            'password', 'dob', 'date_of_birth', 'address', 'salary'
        }

    def classify_column(self, column_name: str, sample_values: List[str],
                       data_type: str = "string") -> Classification:
        detected = []
        confidence = 0.0
        col_lower = column_name.lower()

        for keyword in self.sensitive_keywords:
            if keyword in col_lower:
                detected.append(keyword)
                confidence = max(confidence, 0.8)

        for pattern_name, pattern in self.patterns.items():
            for value in sample_values[:100]:
                if value and re.search(pattern, str(value)):
                    detected.append(pattern_name)
                    confidence = max(confidence, 0.9)
                    break

        if detected:
            level = SensitivityLevel.RESTRICTED if confidence > 0.85 else SensitivityLevel.CONFIDENTIAL
        else:
            level = SensitivityLevel.PUBLIC
            confidence = 0.1

        return Classification(
            column_name=column_name,
            data_type=data_type,
            sensitivity_level=level,
            detected_patterns=detected,
            confidence=confidence
        )

    def classify_table(self, columns: Dict[str, Dict]) -> List[Classification]:
        classifications = []
        for col_name, col_info in columns.items():
            sample_values = col_info.get('sample_values', [])
            data_type = col_info.get('data_type', 'string')
            classification = self.classify_column(col_name, sample_values, data_type)
            classifications.append(classification)
        return classifications
