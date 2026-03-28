"""S3 connector for lineage extraction."""

from typing import List, Dict, Any
from .base_connector import BaseConnector


class S3Connector(BaseConnector):
    """Extracts lineage from S3 buckets."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connector_type = "s3"
        self.bucket = config.get("bucket", "")
        self.region = config.get("region", "us-east-1")

    def extract_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from S3 buckets."""
        prefixes = self.get_entities()
        lineage = []
        for prefix in prefixes:
            lineage.append({
                "entity_id": f"s3://{self.bucket}/{prefix}",
                "entity_type": "s3_object",
                "metadata": {
                    "bucket": self.bucket,
                    "prefix": prefix,
                    "region": self.region
                }
            })
        return lineage

    def get_entities(self) -> List[str]:
        """Get list of S3 prefixes."""
        return self.config.get("prefixes", [])
