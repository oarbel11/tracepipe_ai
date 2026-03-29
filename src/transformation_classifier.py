from typing import Dict, Any

class TransformationClassifier:
    def classify(self, column_info: Dict[str, Any]) -> str:
        if column_info.get("transformation_type") == "passthrough":
            return "passthrough"
        
        expr = column_info.get("expression", "")
        
        if "CONCAT" in expr.upper() or "||" in expr:
            return "concatenation"
        elif any(func in expr.upper() for func in ["UPPER", "LOWER", "TRIM"]):
            return "string_operation"
        elif any(op in expr for op in ["+", "-", "*", "/"]):
            return "arithmetic"
        elif "CASE" in expr.upper():
            return "conditional"
        else:
            return "expression"
