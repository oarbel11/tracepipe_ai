from typing import Dict, List, Optional
import json
from dataclasses import dataclass, field

@dataclass
class RuntimeLineageCapture:
    execution_id: str
    job_name: str
    column_flows: List[Dict] = field(default_factory=list)
    spark_plan: Optional[str] = None
    
class SparkPlanAnalyzer:
    def __init__(self):
        self.captures = []
        
    def parse_execution_plan(self, plan_json: str) -> Dict:
        try:
            plan = json.loads(plan_json)
            return self._extract_column_mappings(plan)
        except json.JSONDecodeError:
            return {'error': 'Invalid plan JSON'}
    
    def _extract_column_mappings(self, plan: Dict) -> Dict:
        mappings = {'inputs': [], 'outputs': [], 'transformations': []}
        if 'nodeName' in plan:
            node_type = plan['nodeName']
            if 'Project' in node_type:
                mappings['transformations'].append({
                    'type': 'projection',
                    'columns': self._extract_projection_columns(plan)
                })
            elif 'Aggregate' in node_type:
                mappings['transformations'].append({
                    'type': 'aggregation',
                    'columns': self._extract_agg_columns(plan)
                })
        if 'children' in plan:
            for child in plan['children']:
                child_mappings = self._extract_column_mappings(child)
                mappings['transformations'].extend(child_mappings['transformations'])
        return mappings
    
    def _extract_projection_columns(self, plan: Dict) -> List[str]:
        cols = []
        if 'output' in plan:
            for col_info in plan['output']:
                if isinstance(col_info, dict) and 'name' in col_info:
                    cols.append(col_info['name'])
        return cols
    
    def _extract_agg_columns(self, plan: Dict) -> List[str]:
        return self._extract_projection_columns(plan)
    
    def capture_runtime_lineage(self, execution_id: str, job_name: str, 
                               spark_plan: str) -> RuntimeLineageCapture:
        capture = RuntimeLineageCapture(
            execution_id=execution_id,
            job_name=job_name,
            spark_plan=spark_plan
        )
        plan_data = self.parse_execution_plan(spark_plan)
        capture.column_flows = plan_data.get('transformations', [])
        self.captures.append(capture)
        return capture
    
    def get_column_flow(self, execution_id: str) -> Optional[RuntimeLineageCapture]:
        for capture in self.captures:
            if capture.execution_id == execution_id:
                return capture
        return None
    
    def export_captures(self) -> List[Dict]:
        return [{
            'execution_id': c.execution_id,
            'job_name': c.job_name,
            'column_flows': c.column_flows
        } for c in self.captures]
