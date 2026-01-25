"""CloudWatch metrics helper for SchemaHub.

This module provides a BatchedMetricsClient that reduces CloudWatch costs by:
1. Batching metric publishes (up to 1000 per API call)
2. Using product bucketing (top 5 products + "other" bucket)
3. Simplifying error metrics to Source-only dimensions
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

from schemahub.config import (
    METRICS_BATCH_THRESHOLD,
    METRICS_OTHER_BUCKET,
    TOP_PRODUCTS_FOR_METRICS,
)

logger = logging.getLogger(__name__)

# Namespace for all SchemaHub metrics
NAMESPACE = "SchemaHub"


def _bucket_product(product_id: str) -> str:
    """Return product_id for top products, 'other' for the rest.

    This reduces CloudWatch metric cardinality from 100+ unique products
    to just 6 (top 5 + "other"), cutting costs significantly.
    """
    if product_id in TOP_PRODUCTS_FOR_METRICS:
        return product_id
    return METRICS_OTHER_BUCKET


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

        Uses product bucketing to reduce CloudWatch cardinality:
        - Top 5 products (BTC-USD, ETH-USD, etc.) get individual metrics
        - All other products are aggregated into "other" bucket

        Args:
            product_id: Trading pair (e.g., BTC-USD)
            trade_count: Number of trades ingested
            source: Exchange name
        """
        bucketed_product = _bucket_product(product_id)
        self._put_metric(
            metric_name="ProductIngestCount",
            value=trade_count,
            unit="Count",
            dimensions=[
                {"Name": "Product", "Value": bucketed_product},
                {"Name": "Source", "Value": source}
            ]
        )
        # Note: ProductLastIngest removed - use put_avg_data_freshness() instead
    
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
    
    def put_ingest_failure(self, source: str) -> None:
        """Record failed ingest.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="IngestFailure",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )
    
    def put_transform_success(self, product_id: str, record_count: int) -> None:
        """Record successful transform for a product.

        Uses product bucketing to reduce CloudWatch cardinality.

        Args:
            product_id: Trading pair
            record_count: Number of records transformed
        """
        bucketed_product = _bucket_product(product_id)
        self._put_metric(
            metric_name="TransformRecords",
            value=record_count,
            unit="Count",
            dimensions=[{"Name": "Product", "Value": bucketed_product}]
        )
    
    def put_data_freshness(self, product_id: str, age_minutes: float) -> None:
        """Record how old the latest data is for a product.

        Uses product bucketing to reduce CloudWatch cardinality.

        Args:
            product_id: Trading pair
            age_minutes: Minutes since last trade
        """
        bucketed_product = _bucket_product(product_id)
        self._put_metric(
            metric_name="DataAgeMinutes",
            value=age_minutes,
            unit="None",
            dimensions=[{"Name": "Product", "Value": bucketed_product}]
        )

    def put_avg_data_freshness(self, source: str, avg_age_mins: float) -> None:
        """Record average data age across all products (single aggregated metric).

        This replaces per-product ProductLastIngest metrics with a single
        aggregated freshness metric to reduce CloudWatch costs.

        Args:
            source: Exchange name
            avg_age_mins: Average minutes since last trade across all products
        """
        self._put_metric(
            metric_name="DataAgeMins",
            value=avg_age_mins,
            unit="None",
            dimensions=[{"Name": "Source", "Value": source}]
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

    def put_exchange_response_time(self, source: str, response_time_ms: float) -> None:
        """Record exchange API response time.

        Args:
            source: Exchange name
            response_time_ms: Response time in milliseconds
        """
        self._put_metric(
            metric_name="ExchangeResponseTime",
            value=response_time_ms,
            unit="Milliseconds",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_exchange_error_rate(self, source: str, error_rate: float) -> None:
        """Record exchange API error rate.

        Args:
            source: Exchange name
            error_rate: Error rate (0.0 to 1.0)
        """
        self._put_metric(
            metric_name="ExchangeErrorRate",
            value=error_rate,
            unit="None",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_circuit_breaker_state(self, source: str, state: str) -> None:
        """Record circuit breaker state.

        Args:
            source: Exchange name
            state: Circuit state ("closed" | "open" | "half_open")
        """
        # Map state to numeric value for easier graphing
        state_value = {"closed": 0, "open": 1, "half_open": 0.5}.get(state, 0)

        self._put_metric(
            metric_name="CircuitBreakerState",
            value=state_value,
            unit="None",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_circuit_breaker_open(self, source: str) -> None:
        """Record circuit breaker opening event (counter).

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="CircuitBreakerOpens",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_rate_limit_error(self, source: str) -> None:
        """Record a 429 rate limit error.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="RateLimitErrors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_server_error(self, source: str) -> None:
        """Record a 5xx server error.

        Note: StatusCode dimension removed to reduce CloudWatch cardinality.
        Individual status codes are still logged for debugging.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="ServerErrors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_timeout_error(self, source: str) -> None:
        """Record a timeout error.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="TimeoutErrors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_connection_error(self, source: str) -> None:
        """Record a connection error.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="ConnectionErrors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "Source", "Value": source}]
        )

    def put_api_success(self, source: str) -> None:
        """Record a successful API call.

        Args:
            source: Exchange name
        """
        self._put_metric(
            metric_name="APISuccessCount",
            value=1,
            unit="Count",
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


class BatchedMetricsClient(MetricsClient):
    """Metrics client that batches PutMetricData calls for cost efficiency.

    CloudWatch allows up to 1000 metrics per PutMetricData call. This class
    buffers metrics and flushes them when the threshold is reached or when
    flush() is called explicitly.

    Usage:
        metrics = BatchedMetricsClient()
        metrics.put_api_success("coinbase")
        metrics.put_exchange_response_time("coinbase", 150.0)
        # ... more metrics ...
        metrics.flush()  # Publish all buffered metrics
    """

    def __init__(
        self,
        enabled: bool = True,
        region: Optional[str] = None,
        flush_threshold: int = METRICS_BATCH_THRESHOLD,
    ):
        """Initialize batched metrics client.

        Args:
            enabled: If False, metrics are logged but not published
            region: AWS region (defaults to AWS_REGION env var or us-east-1)
            flush_threshold: Number of metrics to buffer before auto-flush
        """
        super().__init__(enabled, region)
        self._buffer: List[dict] = []
        self._buffer_lock = threading.Lock()
        self.flush_threshold = flush_threshold

    def _put_metric(
        self,
        metric_name: str,
        value: float,
        unit: str,
        dimensions: list
    ) -> None:
        """Buffer metric instead of publishing immediately."""
        logger.debug(f"Metric (buffered): {metric_name}={value} {dimensions}")

        if not self.enabled:
            logger.info(f"[METRICS DISABLED] {metric_name}={value} {dimensions}")
            return

        metric_datum = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.now(timezone.utc),
            "Dimensions": dimensions
        }

        with self._buffer_lock:
            self._buffer.append(metric_datum)
            buffer_size = len(self._buffer)

        if buffer_size >= self.flush_threshold:
            self.flush()

    def flush(self) -> None:
        """Flush buffered metrics to CloudWatch in batches of 1000."""
        with self._buffer_lock:
            if not self._buffer:
                return
            buffer_copy = self._buffer[:]
            self._buffer = []

        # CloudWatch limit: 1000 metrics per call
        BATCH_SIZE = 1000
        for i in range(0, len(buffer_copy), BATCH_SIZE):
            batch = buffer_copy[i:i + BATCH_SIZE]
            try:
                self.client.put_metric_data(
                    Namespace=NAMESPACE,
                    MetricData=batch
                )
                logger.debug(f"Flushed {len(batch)} metrics to CloudWatch")
            except ClientError as e:
                logger.warning(f"Failed to flush metrics batch: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error flushing metrics: {e}")

    def __del__(self):
        """Ensure final flush on cleanup."""
        try:
            self.flush()
        except Exception:
            pass  # Ignore errors during cleanup


# Global instance for convenience
_metrics_client: Optional[BatchedMetricsClient] = None


def get_metrics_client(enabled: bool = True) -> BatchedMetricsClient:
    """Get or create the global batched metrics client.

    The batched client reduces CloudWatch API costs by buffering metrics
    and publishing them in batches of up to 1000 per API call.

    Args:
        enabled: Whether to actually publish metrics

    Returns:
        BatchedMetricsClient instance
    """
    global _metrics_client
    if _metrics_client is None:
        _metrics_client = BatchedMetricsClient(enabled=enabled)
    return _metrics_client


def flush_metrics() -> None:
    """Flush any buffered metrics to CloudWatch.

    Call this at the end of each ingest run to ensure all metrics are published.
    """
    global _metrics_client
    if _metrics_client is not None:
        _metrics_client.flush()
