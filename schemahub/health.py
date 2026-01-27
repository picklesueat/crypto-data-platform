"""Exchange health tracking and circuit breaker for API reliability."""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Configuration constants
MAX_RETRIES = 5  # Total retry attempts (same as existing limit)
CIRCUIT_OPEN_WAIT_SECONDS = 10  # Initial cooldown: 10 seconds
MAX_CIRCUIT_WAIT_SECONDS = 120  # Max cooldown after repeated failures (backoff caps here)
SUCCESS_THRESHOLD = 3  # Close circuit after 3 consecutive successes
DEGRADED_ERROR_RATE = 0.1  # 10% error rate = degraded
UNHEALTHY_ERROR_RATE = 0.3  # 30% error rate = unhealthy
ROLLING_WINDOW_SIZE = 100  # Track last 100 requests for error rate


@dataclass
class ExchangeHealth:
    """Health state for an exchange API."""

    exchange_name: str
    timestamp: str  # ISO8601
    status: str = "healthy"  # "healthy" | "degraded" | "unhealthy"
    circuit_state: str = "closed"  # "closed" | "open" | "half_open"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_success_ts: Optional[str] = None
    last_failure_ts: Optional[str] = None
    last_error_message: Optional[str] = None
    avg_response_time_ms: float = 0.0
    error_rate: float = 0.0
    request_count: int = 0
    ttl: int = 0  # Unix timestamp for DynamoDB TTL
    reopen_count: int = 0  # Track consecutive reopens for exponential backoff

    # Rolling window for error rate calculation (not stored in DynamoDB)
    recent_results: list = field(default_factory=list, repr=False)

    @classmethod
    def from_dynamodb(cls, item: dict) -> "ExchangeHealth":
        """Create ExchangeHealth from DynamoDB item."""
        return cls(
            exchange_name=item.get("exchange_name", ""),
            timestamp=item.get("timestamp", ""),
            status=item.get("status", "healthy"),
            circuit_state=item.get("circuit_state", "closed"),
            consecutive_failures=int(item.get("consecutive_failures", 0)),
            consecutive_successes=int(item.get("consecutive_successes", 0)),
            last_success_ts=item.get("last_success_ts"),
            last_failure_ts=item.get("last_failure_ts"),
            last_error_message=item.get("last_error_message"),
            avg_response_time_ms=float(item.get("avg_response_time_ms", 0.0)),
            error_rate=float(item.get("error_rate", 0.0)),
            request_count=int(item.get("request_count", 0)),
            ttl=int(item.get("ttl", 0)),
            reopen_count=int(item.get("reopen_count", 0)),
        )

    def to_dynamodb(self) -> dict:
        """Convert to DynamoDB item format (using Decimal for floats)."""
        item = {
            "exchange_name": self.exchange_name,
            "timestamp": self.timestamp,
            "status": self.status,
            "circuit_state": self.circuit_state,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "avg_response_time_ms": Decimal(str(self.avg_response_time_ms)),
            "error_rate": Decimal(str(self.error_rate)),
            "request_count": self.request_count,
            "ttl": self.ttl,
            "reopen_count": self.reopen_count,
        }

        # Optional fields
        if self.last_success_ts:
            item["last_success_ts"] = self.last_success_ts
        if self.last_failure_ts:
            item["last_failure_ts"] = self.last_failure_ts
        if self.last_error_message:
            item["last_error_message"] = self.last_error_message

        return item


class ExchangeHealthTracker:
    """Manages DynamoDB operations for exchange health state."""

    def __init__(self, table_name: Optional[str] = None, region: Optional[str] = None):
        """Initialize health tracker.

        Args:
            table_name: DynamoDB table name (defaults to env var DYNAMODB_HEALTH_TABLE)
            region: AWS region (defaults to AWS_REGION env var or us-east-1)
        """
        self.table_name = table_name or os.getenv("DYNAMODB_HEALTH_TABLE", "schemahub-exchange-health")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.enabled = os.getenv("HEALTH_CHECK_ENABLED", "true").lower() == "true"
        self._dynamodb = None
        self._table = None

    @property
    def dynamodb(self):
        """Lazy-load DynamoDB resource."""
        if self._dynamodb is None and self.enabled:
            self._dynamodb = boto3.resource("dynamodb", region_name=self.region)
        return self._dynamodb

    @property
    def table(self):
        """Lazy-load DynamoDB table."""
        if self._table is None and self.enabled and self.dynamodb:
            self._table = self.dynamodb.Table(self.table_name)
        return self._table

    def get_health(self, exchange: str) -> ExchangeHealth:
        """Get current health state for an exchange.

        Returns the most recent health record, or a new healthy state if none exists.
        """
        if not self.enabled:
            # Health checks disabled - return default healthy state
            return ExchangeHealth(
                exchange_name=exchange,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        try:
            # Query for latest record (sort key descending)
            response = self.table.query(
                KeyConditionExpression="exchange_name = :exchange",
                ExpressionAttributeValues={":exchange": exchange},
                ScanIndexForward=False,  # Descending order (latest first)
                Limit=1,
            )

            items = response.get("Items", [])
            if items:
                return ExchangeHealth.from_dynamodb(items[0])
            else:
                # No existing record - return default healthy state
                logger.info(f"No health record found for {exchange} - initializing healthy state")
                return ExchangeHealth(
                    exchange_name=exchange,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )

        except ClientError as e:
            logger.error(f"Failed to get health state for {exchange}: {e}")
            # Return default healthy state on error
            return ExchangeHealth(
                exchange_name=exchange,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

    def update_health(self, health: ExchangeHealth) -> None:
        """Update health state in DynamoDB.

        Creates a new item with current timestamp.
        """
        if not self.enabled:
            logger.debug(f"Health check disabled - skipping update for {health.exchange_name}")
            return

        # Set TTL to 7 days from now
        ttl_datetime = datetime.now(timezone.utc) + timedelta(days=7)
        health.ttl = int(ttl_datetime.timestamp())

        # Set timestamp to now
        health.timestamp = datetime.now(timezone.utc).isoformat()

        try:
            self.table.put_item(Item=health.to_dynamodb())
            logger.debug(f"Updated health state for {health.exchange_name}: {health.circuit_state}")
        except ClientError as e:
            logger.error(f"Failed to update health state for {health.exchange_name}: {e}")

    def conditional_transition(
        self, exchange: str, expected_state: str, new_state: str
    ) -> bool:
        """Conditionally transition circuit state (atomic operation).

        Used to ensure only ONE thread transitions from OPEN to HALF_OPEN.

        Returns:
            True if transition succeeded, False if condition failed
        """
        if not self.enabled:
            return True  # Allow transition if health checks disabled

        try:
            # Get current health
            health = self.get_health(exchange)

            # Attempt atomic conditional update
            timestamp = datetime.now(timezone.utc).isoformat()
            ttl = int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())

            item = {
                "exchange_name": exchange,
                "timestamp": timestamp,
                "circuit_state": new_state,
                "status": health.status,  # Preserve status
                "consecutive_failures": health.consecutive_failures,
                "consecutive_successes": health.consecutive_successes,
                "avg_response_time_ms": Decimal(str(health.avg_response_time_ms)),
                "error_rate": Decimal(str(health.error_rate)),
                "request_count": health.request_count,
                "ttl": ttl,
            }
            # Add optional fields only if set
            if health.last_success_ts:
                item["last_success_ts"] = health.last_success_ts
            if health.last_failure_ts:
                item["last_failure_ts"] = health.last_failure_ts
            if health.last_error_message:
                item["last_error_message"] = health.last_error_message

            self.table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(#ts) OR circuit_state = :expected_state",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExpressionAttributeValues={":expected_state": expected_state},
            )

            logger.info(f"{exchange} circuit: {expected_state} → {new_state} (atomic transition)")
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.debug(f"{exchange} circuit transition failed - another thread already transitioned")
                return False
            else:
                logger.error(f"Failed conditional transition for {exchange}: {e}")
                return False


class CircuitBreaker:
    """Circuit breaker pattern for exchange APIs."""

    def __init__(self, health_tracker: Optional[ExchangeHealthTracker] = None):
        """Initialize circuit breaker.

        Args:
            health_tracker: ExchangeHealthTracker instance (creates default if None)
        """
        self.health_tracker = health_tracker or ExchangeHealthTracker()
        self.enabled = os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"

    def get_wait_time(self, exchange: str, attempt: int) -> int:
        """Get wait time before next retry based on circuit state.

        Returns:
            Wait time in seconds (0 = proceed immediately)
        """
        if not self.enabled:
            return 0  # No wait if circuit breaker disabled

        health = self.health_tracker.get_health(exchange)

        if health.circuit_state == "closed":
            # Normal operation - no additional wait
            return 0

        elif health.circuit_state == "open":
            # Circuit is OPEN - calculate dynamic cooldown with exponential backoff
            if not health.last_failure_ts:
                logger.warning(f"{exchange} circuit OPEN but no last_failure_ts - proceeding")
                return 0

            last_failure = datetime.fromisoformat(health.last_failure_ts.replace("Z", "+00:00"))
            time_since_failure = (datetime.now(timezone.utc) - last_failure).total_seconds()

            # Calculate cooldown with exponential backoff: 10s, 20s, 40s, 80s, max 120s
            cooldown = min(
                CIRCUIT_OPEN_WAIT_SECONDS * (2 ** health.reopen_count),
                MAX_CIRCUIT_WAIT_SECONDS
            )

            if time_since_failure >= cooldown:
                # Cooldown complete - try to transition to HALF_OPEN
                if self.should_test_recovery(exchange):
                    # This thread won the race - test recovery
                    logger.info(f"{exchange} circuit → HALF_OPEN (testing recovery after {cooldown}s cooldown)")
                    return 0  # Proceed immediately
                else:
                    # Another thread is testing - wait a bit
                    logger.info(f"{exchange} circuit testing by another thread - waiting 30s")
                    return 30
            else:
                # Still in cooldown - wait for remaining time
                remaining = int(cooldown - time_since_failure)
                logger.warning(f"{exchange} circuit OPEN - {remaining}s until retry (cooldown={cooldown}s, reopen_count={health.reopen_count})")
                return remaining

        elif health.circuit_state == "half_open":
            # Circuit is testing recovery - allow requests
            return 0

        # Unknown state - proceed
        return 0

    def should_test_recovery(self, exchange: str) -> bool:
        """Determine if THIS thread should test recovery (atomic operation).

        Uses DynamoDB conditional write to ensure only ONE thread tests.
        """
        return self.health_tracker.conditional_transition(exchange, "open", "half_open")

    def record_success(self, exchange: str, response_time_ms: float) -> None:
        """Record successful API call - may close circuit."""
        health = self.health_tracker.get_health(exchange)

        # Update counters
        health.consecutive_successes += 1
        health.consecutive_failures = 0  # Reset
        health.last_success_ts = datetime.now(timezone.utc).isoformat()
        health.request_count += 1

        # Update response time (moving average)
        if health.avg_response_time_ms == 0:
            health.avg_response_time_ms = response_time_ms
        else:
            # Exponential moving average (alpha = 0.2)
            health.avg_response_time_ms = 0.8 * health.avg_response_time_ms + 0.2 * response_time_ms

        # Update rolling window for error rate
        health.recent_results.append(True)
        if len(health.recent_results) > ROLLING_WINDOW_SIZE:
            health.recent_results.pop(0)

        # Recalculate error rate
        if health.recent_results:
            failures = sum(1 for r in health.recent_results if not r)
            health.error_rate = failures / len(health.recent_results)

        # State transition logic
        if health.circuit_state == "half_open":
            # Testing recovery - check if we have enough successes
            if health.consecutive_successes >= SUCCESS_THRESHOLD:
                health.circuit_state = "closed"
                health.status = "healthy"
                health.reopen_count = 0  # Reset backoff on successful recovery
                logger.info(f"{exchange} circuit CLOSED - exchange recovered! ({health.consecutive_successes} consecutive successes, reopen_count reset)")

        elif health.circuit_state == "closed":
            # Update health status based on error rate
            if health.error_rate < DEGRADED_ERROR_RATE:
                health.status = "healthy"
            elif health.error_rate < UNHEALTHY_ERROR_RATE:
                health.status = "degraded"
            else:
                health.status = "unhealthy"

        # Publish CloudWatch metrics
        from schemahub.metrics import get_metrics_client
        metrics_client = get_metrics_client()
        metrics_client.put_exchange_error_rate(exchange, health.error_rate)
        metrics_client.put_circuit_breaker_state(exchange, health.circuit_state)

        # Save to DynamoDB
        self.health_tracker.update_health(health)

    def record_failure(self, exchange: str, error_msg: str) -> None:
        """Record failed API call - may open circuit."""
        health = self.health_tracker.get_health(exchange)

        # Update counters
        health.consecutive_failures += 1
        health.consecutive_successes = 0  # Reset
        health.last_failure_ts = datetime.now(timezone.utc).isoformat()
        health.last_error_message = error_msg[:500]  # Truncate long errors
        health.request_count += 1

        # Update rolling window for error rate
        health.recent_results.append(False)
        if len(health.recent_results) > ROLLING_WINDOW_SIZE:
            health.recent_results.pop(0)

        # Recalculate error rate
        if health.recent_results:
            failures = sum(1 for r in health.recent_results if not r)
            health.error_rate = failures / len(health.recent_results)

        # State transition logic
        circuit_opened = False
        if health.circuit_state == "closed":
            # Check if we should open the circuit
            if health.consecutive_failures >= MAX_RETRIES:
                health.circuit_state = "open"
                health.status = "unhealthy"
                circuit_opened = True
                logger.error(
                    f"{exchange} circuit OPENED after {health.consecutive_failures} "
                    f"consecutive failures. Last error: {error_msg}"
                )

        elif health.circuit_state == "half_open":
            # Recovery test failed - go back to OPEN
            health.circuit_state = "open"
            health.status = "unhealthy"
            circuit_opened = True
            logger.error(f"{exchange} recovery test failed - circuit reopened. Error: {error_msg}")

        # Increment reopen_count for exponential backoff
        if circuit_opened:
            health.reopen_count += 1
            logger.info(f"{exchange} reopen_count incremented to {health.reopen_count} (next cooldown: {min(CIRCUIT_OPEN_WAIT_SECONDS * (2 ** health.reopen_count), MAX_CIRCUIT_WAIT_SECONDS)}s)")

        # Publish CloudWatch metrics
        from schemahub.metrics import get_metrics_client
        metrics_client = get_metrics_client()
        metrics_client.put_exchange_error_rate(exchange, health.error_rate)
        metrics_client.put_circuit_breaker_state(exchange, health.circuit_state)
        if circuit_opened:
            metrics_client.put_circuit_breaker_open(exchange)

        # Save to DynamoDB
        self.health_tracker.update_health(health)


# Global instances
_health_tracker: Optional[ExchangeHealthTracker] = None
_circuit_breaker: Optional[CircuitBreaker] = None


def get_health_tracker() -> ExchangeHealthTracker:
    """Get or create global health tracker instance."""
    global _health_tracker
    if _health_tracker is None:
        _health_tracker = ExchangeHealthTracker()
    return _health_tracker


def get_circuit_breaker() -> CircuitBreaker:
    """Get or create global circuit breaker instance."""
    global _circuit_breaker
    if _circuit_breaker is None:
        _circuit_breaker = CircuitBreaker(get_health_tracker())
    return _circuit_breaker
