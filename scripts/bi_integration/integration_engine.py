import json
from typing import Dict, List, Optional
from .bi_metadata_extractor import BIMetadataExtractor, BIMetric
from .metric_mapper import MetricToTableMapper

class BIIntegrationEngine:
    def __init__(self, catalog: str = 'companies_data'):
        self.catalog = catalog
        self.mapper = MetricToTableMapper(catalog)
        self.metadata_store = []

    def sync_bi_metadata(self, platform: str, workspace: str,
                         credentials: Optional[Dict] = None) -> int:
        if credentials is None:
            credentials = {}
        extractor = BIMetadataExtractor(platform)
        metrics = extractor.extract_metadata(workspace, credentials)
        self.metadata_store.extend(metrics)
        return len(metrics)

    def trace_metric_to_source(self, metric_name: str, 
                               dashboard: str = None) -> Dict:
        matching = [m for m in self.metadata_store if m.name == metric_name]
        if dashboard:
            matching = [m for m in matching if m.dashboard == dashboard]
        
        if not matching:
            return {'error': f'Metric {metric_name} not found'}
        
        metric = matching[0]
        lineage = self.mapper.trace_metric(
            metric.name, metric.upstream_tables, metric.upstream_columns
        )
        lineage['definition'] = metric.definition
        lineage['dashboard'] = metric.dashboard
        lineage['platform'] = metric.platform
        return lineage

    def get_all_metrics(self) -> List[Dict]:
        return [{'name': m.name, 'dashboard': m.dashboard, 'platform': m.platform}
                for m in self.metadata_store]

    def validate_all_metrics(self) -> Dict:
        results = {'valid': [], 'invalid': []}
        for metric in self.metadata_store:
            valid = True
            for table in metric.upstream_tables:
                for col in metric.upstream_columns:
                    if not self.mapper.validate_metric_source(table, col):
                        valid = False
                        break
            if valid:
                results['valid'].append(metric.name)
            else:
                results['invalid'].append(metric.name)
        return results
