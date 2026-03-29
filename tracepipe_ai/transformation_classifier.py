from typing import Dict, Any

class TransformationClassifier:
    CATEGORIES = {
        "simple": ["passthrough", "rename"],
        "string": ["concat", "substring", "upper", "lower", "trim"],
        "numeric": ["sum", "avg", "count", "min", "max", "round"],
        "conditional": ["case", "if", "coalesce"],
        "type_conversion": ["cast", "convert"],
        "date": ["date_format", "date_add", "date_sub", "extract"],
        "complex": ["expression", "udf", "window"]
    }

    def classify(self, transformation_type: str) -> Dict[str, Any]:
        category = "complex"
        for cat, types in self.CATEGORIES.items():
            if transformation_type in types:
                category = cat
                break
        
        complexity_score = self._calculate_complexity(transformation_type, category)
        
        return {
            "category": category,
            "complexity_score": complexity_score,
            "reversible": category in ["simple", "type_conversion"],
            "data_quality_risk": complexity_score > 5
        }

    def _calculate_complexity(self, transformation_type: str, category: str) -> int:
        complexity_map = {
            "simple": 1,
            "string": 3,
            "numeric": 3,
            "conditional": 5,
            "type_conversion": 2,
            "date": 4,
            "complex": 8
        }
        return complexity_map.get(category, 5)
