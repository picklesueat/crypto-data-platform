"""CloudWatch metrics helper for SchemaHub."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Namespace for all SchemaHub metrics
NAMESPACE = "SchemaHub"


class MetricsClient:
    """Publishes custom metrics to CloudWatch."""
    
    def __init__(self, enabled: bool = True, region: Optional[str] = None):
        """Initialize metrics client.
        
        Args:
            enabled: If False, metrics are logged but not published (for local dev)
            region: AWS region (defaults to AWS_REGION env var or us-east-1)
        """
        self.enabled = enabled
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self._client = None
    
    @property
    def client(self):
        """Lazy-load CloudWatch client."""
        if self._client is None and self.enabled:
            self._client = boto3.client("cloudwatch", region_name=self.region)
        return self._client
    
    def put_product_ingest(
        self,
        product_id: str,
        trade_count: int,
        source: str = "coinbase"
    ) -> None:
        """Record trades ingested for a specific product.
        
        Args:
            product_id: Trading pair (e.g., BTC-USD)
            trade_count: Number of trades ingested
            source: Exchange name
        """
        self._put_metric(
            metric_name="ProductIngestCount",
            value=trade_count,
            unit="Count",
            dimensions=[
                {"Name": "Product", "Value": product_id},
                {"Name": "Source", "Value": source}
            ]
        )
        
        # Also record that this product was just ingested (for freshness tracking)
        self._put_metric(
            metric_name="ProductLastIngest",
            value=0,  # 0 minutes ago (just now)
            unit="None",
            dimensions=[
                {"Name": "Product", "Value": product_id},
                {"Name": "Source", "Value": source}
            ]
        )
    
    def put_ingest_success(self, source: str, product_count: int, total_trades: int) -> None:
        """Record successful ingest run.
        
        Args:
            source: Exchange name
            product_count: Number of products ingested
            total_trades: Total trades across all products
        """
        self._put_metric(
            metric_name="IngestSuccess",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )
        self._put_metric(
            metric_name="IngestProductCount",
            value=product_count,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )
        self._put_metric(
            metric_name="IngestTotalTrades",
            value=total_trades,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )
    
    def put_ingest_failure(self, source: str, product_id: Optional[str] = None) -> None:
        """Record failed ingest.
        
        Args:
            source: Exchange name
            product_id: Optional - specific product that failed
        """
        dimensions = [{"Name": "Source", "Value": source}]
        if product_id:
            dimensions.append({"Name": "Product", "Value": product_id})
        
        self._put_metric(
            metric_name="IngestFailure",
            value=1,
            unit="Count",
            dimensions=dimensions
        )
    
    def put_transform_success(self, product_id: str, record_count: int) -> None:
        """Record successful transform for a product.
        
        Args:
            product_id: Trading pair
            record_count: Number of records transformed
        """
        self._put_metric(
            metric_name="TransformRecords",
            value=record_count,
            unit="Count",
            dimensions=[{"Name": "Product", "Value": product_id}]
        )
    
    def put_data_freshness(self, product_id: str, age_minutes: float) -> None:
        """Record how old the latest data is for a product.
        
        Args:
            product_id: Trading pair
            age_minutes: Minutes since last trade
        """
        self._put_metric(
            metric_name="DataAgeMinutes",
            value=age_minutes,
            unit="None",
            dimensions=[{"Name": "Product", "Value": product_id}]
        )
    
    def put_exchange_status(self, source: str, is_healthy: bool) -> None:
        """Record exchange connection status.
        
        Args:
            source: Exchange name
            is_healthy: True if exchange API is responding
        """
        self._put_metric(
            metric_name="ExchangeHealthy",
            value=1 if is_healthy else 0,
            unit="None",
            dimensions=[{"Name": "Source", "Value": source}]
        )
    
    def _put_metric(
        self,
        metric_name: str,
        value: float,
        unit: str,
        dimensions: list
    ) -> None:
        """Internal method to publish a metric to CloudWatch.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit (Count, None, Seconds, etc.)
            dimensions: List of dimension dicts with Name and Value
        """
        logger.debug(f"Metric: {metric_name}={value} {dimensions}")
        
        if not self.enabled:
            logger.info(f"[METRICS DISABLED] {metric_name}={value} {dimensions}")
            return
        
        try:
            self.client.put_metric_data(
                Namespace=NAMESPACE,
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit,
                        "Timestamp": datetime.now(timezone.utc),
                        "Dimensions": dimensions
                    }
                ]
            )
        except ClientError as e:
            logger.warning(f"Failed to publish metric {metric_name}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error publishing metric {metric_name}: {e}")


# Global instance for convenience
_metrics_client: Optional[MetricsClient] = None


def get_metrics_client(enabled: bool = True) -> MetricsClient:
    """Get or create the global metrics client.
    
    Args:
        enabled: Whether to actually publish metrics
    
    Returns:
        MetricsClient instance
    """
    global _metrics_client
    if _metrics_client is None:
        _metrics_client = MetricsClient(enabled=enabled)
    return _metrics_client
