from pathlib import Path
from typing import Dict, List
import json

from .unmanaged_capture import UnmanagedLineageCapture
from .udf_mapper import UDFColumnMapper


class LineageOrchestrator:
    def __init__(self, repo_path: str = None):
        self.repo_path = Path(repo_path) if repo_path else Path.cwd()
        self.unmanaged_capture = UnmanagedLineageCapture()
        self.udf_mapper = UDFColumnMapper()
        self.lineage_graph = {'nodes': [], 'edges': []}
    
    def scan_repository(self) -> Dict:
        all_lineage = {
            'unmanaged_writes': [],
            'udf_mappings': [],
            'summary': {}
        }
        
        for sql_file in self.repo_path.rglob('*.sql'):
            content = sql_file.read_text()
            all_lineage['unmanaged_writes'].extend(
                self.unmanaged_capture.extract_from_sql(content)
            )
            all_lineage['udf_mappings'].extend(
                self.udf_mapper.extract_udf_lineage('', content)
            )
        
        for py_file in self.repo_path.rglob('*.py'):
            if 'venv' in str(py_file) or '__pycache__' in str(py_file):
                continue
            
            try:
                content = py_file.read_text()
                all_lineage['unmanaged_writes'].extend(
                    self.unmanaged_capture.extract_from_python(content)
                )
                all_lineage['udf_mappings'].extend(
                    self.udf_mapper.extract_udf_lineage(content)
                )
            except Exception:
                continue
        
        all_lineage['summary'] = {
            'total_unmanaged_writes': len(all_lineage['unmanaged_writes']),
            'total_udfs': len(all_lineage['udf_mappings']),
            'scanned_path': str(self.repo_path)
        }
        
        return all_lineage
    
    def analyze_file(self, file_path: str) -> Dict:
        path = Path(file_path)
        content = path.read_text()
        
        result = {
            'file': str(path),
            'unmanaged_writes': [],
            'udf_mappings': []
        }
        
        if path.suffix == '.sql':
            result['unmanaged_writes'] = self.unmanaged_capture.extract_from_sql(content)
            result['udf_mappings'] = self.udf_mapper.extract_udf_lineage('', content)
        elif path.suffix == '.py':
            result['unmanaged_writes'] = self.unmanaged_capture.extract_from_python(content)
            result['udf_mappings'] = self.udf_mapper.extract_udf_lineage(content)
        
        return result
    
    def export_lineage(self, output_path: str):
        lineage_data = self.scan_repository()
        
        with open(output_path, 'w') as f:
            json.dump(lineage_data, f, indent=2)
        
        return output_path
