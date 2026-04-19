from typing import Dict, List, Any
import statistics

class AnomalyDetector:
    def __init__(self, sensitivity: float = 2.0):
        self.sensitivity = sensitivity

    def detect_schema_changes(self, baseline: Dict, current: Dict) -> Dict:
        baseline_cols = set(baseline.get('columns', []))
        current_cols = set(current.get('columns', []))
        
        added = list(current_cols - baseline_cols)
        removed = list(baseline_cols - current_cols)
        
        baseline_types = baseline.get('column_types', {})
        current_types = current.get('column_types', {})
        type_changes = []
        for col in baseline_cols & current_cols:
            if baseline_types.get(col) != current_types.get(col):
                type_changes.append({
                    'column': col,
                    'old_type': baseline_types.get(col),
                    'new_type': current_types.get(col)
                })
        
        return {
            'added_columns': added,
            'removed_columns': removed,
            'type_changes': type_changes
        }

    def detect_data_quality_anomalies(self, historical_stats: List[Dict], 
                                     current_stats: Dict) -> List[Dict]:
        anomalies = []
        if len(historical_stats) < 3:
            return anomalies
        
        for metric, current_value in current_stats.items():
            if not isinstance(current_value, (int, float)):
                continue
            
            historical_values = [s.get(metric) for s in historical_stats 
                               if isinstance(s.get(metric), (int, float))]
            
            if len(historical_values) < 3:
                continue
            
            mean = statistics.mean(historical_values)
            stdev = statistics.stdev(historical_values) if len(historical_values) > 1 else 0
            
            if stdev > 0:
                z_score = abs((current_value - mean) / stdev)
                if z_score > self.sensitivity:
                    anomalies.append({
                        'metric': metric,
                        'current_value': current_value,
                        'expected_range': (mean - self.sensitivity * stdev, 
                                         mean + self.sensitivity * stdev),
                        'z_score': z_score
                    })
        
        return anomalies

    def detect_sensitive_data_patterns(self, column_samples: Dict[str, List]) -> Dict:
        sensitive_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'
        }
        
        findings = {}
        for col, samples in column_samples.items():
            detected = []
            for pattern_name, pattern in sensitive_patterns.items():
                import re
                for sample in samples[:100]:
                    if isinstance(sample, str) and re.search(pattern, sample):
                        detected.append(pattern_name)
                        break
            if detected:
                findings[col] = detected
        
        return findings
