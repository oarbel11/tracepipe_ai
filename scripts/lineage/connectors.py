from typing import Dict, List, Any
from .connector_registry import BaseConnector


class KafkaConnector(BaseConnector):
    def get_node_type(self) -> str:
        return "kafka_topic"

    def extract_lineage(self) -> List[Dict[str, Any]]:
        topics = self.config.get('topics', [])
        consumers = self.config.get('consumers', [])
        producers = self.config.get('producers', [])
        
        lineage = []
        for producer in producers:
            for topic in topics:
                lineage.append({
                    'source': producer,
                    'target': f"kafka://{topic}",
                    'metadata': {'source_attrs': {'type': 'application'},
                                'target_attrs': {'type': 'kafka_topic'}}
                })
        
        for topic in topics:
            for consumer in consumers:
                lineage.append({
                    'source': f"kafka://{topic}",
                    'target': consumer,
                    'metadata': {'source_attrs': {'type': 'kafka_topic'},
                                'target_attrs': {'type': 'application'}}
                })
        return lineage


class S3Connector(BaseConnector):
    def get_node_type(self) -> str:
        return "s3_bucket"

    def extract_lineage(self) -> List[Dict[str, Any]]:
        buckets = self.config.get('buckets', [])
        readers = self.config.get('readers', [])
        writers = self.config.get('writers', [])
        
        lineage = []
        for writer in writers:
            for bucket in buckets:
                lineage.append({
                    'source': writer,
                    'target': f"s3://{bucket}",
                    'metadata': {'source_attrs': {'type': 'application'},
                                'target_attrs': {'type': 's3_bucket'}}
                })
        
        for bucket in buckets:
            for reader in readers:
                lineage.append({
                    'source': f"s3://{bucket}",
                    'target': reader,
                    'metadata': {'source_attrs': {'type': 's3_bucket'},
                                'target_attrs': {'type': 'application'}}
                })
        return lineage


class PowerBIConnector(BaseConnector):
    def get_node_type(self) -> str:
        return "powerbi_report"

    def extract_lineage(self) -> List[Dict[str, Any]]:
        reports = self.config.get('reports', [])
        datasets = self.config.get('datasets', [])
        
        return [{'source': ds, 'target': f"powerbi://{report}",
                'metadata': {'source_attrs': {'type': 'dataset'},
                            'target_attrs': {'type': 'powerbi_report'}}}
                for ds in datasets for report in reports]


class TableauConnector(BaseConnector):
    def get_node_type(self) -> str:
        return "tableau_dashboard"

    def extract_lineage(self) -> List[Dict[str, Any]]:
        dashboards = self.config.get('dashboards', [])
        datasources = self.config.get('datasources', [])
        
        return [{'source': ds, 'target': f"tableau://{dashboard}",
                'metadata': {'source_attrs': {'type': 'datasource'},
                            'target_attrs': {'type': 'tableau_dashboard'}}}
                for ds in datasources for dashboard in dashboards]
