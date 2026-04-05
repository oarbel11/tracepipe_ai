from typing import Dict, List, Any
from .metadata_extractor import BIMetadataExtractor
from .metric_mapper import MetricToTableMapper

class BIIntegrationEngine:
    """Main engine for BI tool integration"""
    
    def __init__(self, bi_tool: str, config: Dict[str, Any], lineage_data: Dict[str, Any]):
        self.extractor = BIMetadataExtractor(bi_tool, config)
        self.mapper = MetricToTableMapper(lineage_data)
        self.bi_tool = bi_tool
    
    def sync_metadata(self) -> Dict[str, Any]:
        """Sync BI metadata and map to Unity Catalog"""
        dashboards = self.extractor.extract_dashboards()
        
        results = {
            'dashboards': [],
            'total_metrics': 0,
            'total_mappings': 0
        }
        
        for dashboard in dashboards:
            metrics = dashboard.get('metrics', [])
            dashboard_result = {
                'id': dashboard['id'],
                'name': dashboard['name'],
                'metrics': []
            }
            
            for metric in metrics:
                mappings = self.mapper.map_metric_to_tables(metric)
                dashboard_result['metrics'].append({
                    'name': metric['name'],
                    'mappings': mappings
                })
                results['total_mappings'] += len(mappings)
            
            results['dashboards'].append(dashboard_result)
            results['total_metrics'] += len(metrics)
        
        return results
    
    def query_metric_lineage(self, metric_name: str) -> Dict[str, Any]:
        """Query lineage for a specific metric"""
        dashboards = self.extractor.extract_dashboards()
        
        for dashboard in dashboards:
            for metric in dashboard.get('metrics', []):
                if metric['name'] == metric_name:
                    mappings = self.mapper.map_metric_to_tables(metric)
                    return {
                        'metric': metric_name,
                        'dashboard': dashboard['name'],
                        'mappings': mappings
                    }
        
        return {'metric': metric_name, 'mappings': []}
