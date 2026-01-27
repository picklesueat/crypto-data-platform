"""Unit tests for exchange health tracking and circuit breaker."""
import os
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock

import boto3
from moto import mock_aws

from schemahub.health import (
    ExchangeHealth,
    ExchangeHealthTracker,
    CircuitBreaker,
    MAX_RETRIES,
    CIRCUIT_OPEN_WAIT_SECONDS,
    SUCCESS_THRESHOLD,
    DEGRADED_ERROR_RATE,
    UNHEALTHY_ERROR_RATE,
    ROLLING_WINDOW_SIZE,
    get_health_tracker,
    get_circuit_breaker,
)


# ============================================================================
# ExchangeHealth dataclass tests
# ============================================================================

class TestExchangeHealth:
    """Tests for ExchangeHealth dataclass."""

    def test_default_values(self):
        """Test ExchangeHealth default values."""
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )

        assert health.exchange_name == "coinbase"
        assert health.status == "healthy"
        assert health.circuit_state == "closed"
        assert health.consecutive_failures == 0
        assert health.consecutive_successes == 0
        assert health.avg_response_time_ms == 0.0
        assert health.error_rate == 0.0
        assert health.recent_results == []

    def test_from_dynamodb_complete_item(self):
        """Test creating ExchangeHealth from complete DynamoDB item."""
        item = {
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T00:00:00Z",
            "status": "degraded",
            "circuit_state": "half_open",
            "consecutive_failures": 3,
            "consecutive_successes": 1,
            "last_success_ts": "2024-01-01T01:00:00Z",
            "last_failure_ts": "2024-01-01T00:30:00Z",
            "last_error_message": "API timeout",
            "avg_response_time_ms": Decimal("150.5"),
            "error_rate": Decimal("0.15"),
            "request_count": 100,
            "ttl": 1704153600,
        }

        health = ExchangeHealth.from_dynamodb(item)

        assert health.exchange_name == "coinbase"
        assert health.status == "degraded"
        assert health.circuit_state == "half_open"
        assert health.consecutive_failures == 3
        assert health.consecutive_successes == 1
        assert health.last_success_ts == "2024-01-01T01:00:00Z"
        assert health.last_failure_ts == "2024-01-01T00:30:00Z"
        assert health.last_error_message == "API timeout"
        assert health.avg_response_time_ms == 150.5
        assert health.error_rate == 0.15
        assert health.request_count == 100
        assert health.ttl == 1704153600

    def test_from_dynamodb_missing_optional_fields(self):
        """Test creating ExchangeHealth from DynamoDB item with missing optional fields."""
        item = {
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T00:00:00Z",
        }

        health = ExchangeHealth.from_dynamodb(item)

        assert health.exchange_name == "coinbase"
        assert health.status == "healthy"
        assert health.circuit_state == "closed"
        assert health.last_success_ts is None
        assert health.last_failure_ts is None
        assert health.last_error_message is None

    def test_to_dynamodb_includes_required_fields(self):
        """Test to_dynamodb includes all required fields."""
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            status="healthy",
            circuit_state="closed",
            avg_response_time_ms=100.0,
            error_rate=0.05,
        )

        item = health.to_dynamodb()

        assert item["exchange_name"] == "coinbase"
        assert item["timestamp"] == "2024-01-01T00:00:00Z"
        assert item["status"] == "healthy"
        assert item["circuit_state"] == "closed"
        assert item["avg_response_time_ms"] == Decimal("100.0")
        assert item["error_rate"] == Decimal("0.05")

    def test_to_dynamodb_includes_optional_fields_when_set(self):
        """Test to_dynamodb includes optional fields when they are set."""
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            last_success_ts="2024-01-01T01:00:00Z",
            last_failure_ts="2024-01-01T00:30:00Z",
            last_error_message="API timeout",
        )

        item = health.to_dynamodb()

        assert item["last_success_ts"] == "2024-01-01T01:00:00Z"
        assert item["last_failure_ts"] == "2024-01-01T00:30:00Z"
        assert item["last_error_message"] == "API timeout"

    def test_to_dynamodb_excludes_optional_fields_when_none(self):
        """Test to_dynamodb excludes optional fields when they are None."""
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )

        item = health.to_dynamodb()

        assert "last_success_ts" not in item
        assert "last_failure_ts" not in item
        assert "last_error_message" not in item

    def test_to_dynamodb_decimal_conversion(self):
        """Test that floats are converted to Decimal for DynamoDB."""
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            avg_response_time_ms=123.456789,
            error_rate=0.123456,
        )

        item = health.to_dynamodb()

        assert isinstance(item["avg_response_time_ms"], Decimal)
        assert isinstance(item["error_rate"], Decimal)


# ============================================================================
# ExchangeHealthTracker tests
# ============================================================================

@pytest.fixture
def dynamodb_table():
    """Create a mocked DynamoDB table for testing."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table_name = "test-exchange-health"
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "exchange_name", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "exchange_name", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Set environment variables
        old_table = os.environ.get("DYNAMODB_HEALTH_TABLE")
        old_enabled = os.environ.get("HEALTH_CHECK_ENABLED")
        os.environ["DYNAMODB_HEALTH_TABLE"] = table_name
        os.environ["HEALTH_CHECK_ENABLED"] = "true"

        yield table

        # Restore environment
        if old_table is not None:
            os.environ["DYNAMODB_HEALTH_TABLE"] = old_table
        else:
            os.environ.pop("DYNAMODB_HEALTH_TABLE", None)
        if old_enabled is not None:
            os.environ["HEALTH_CHECK_ENABLED"] = old_enabled
        else:
            os.environ.pop("HEALTH_CHECK_ENABLED", None)


class TestExchangeHealthTracker:
    """Tests for ExchangeHealthTracker with mocked DynamoDB."""

    def test_get_health_returns_default_for_new_exchange(self, dynamodb_table):
        """Test get_health returns default healthy state for new exchange."""
        tracker = ExchangeHealthTracker(table_name="test-exchange-health")

        health = tracker.get_health("coinbase")

        assert health.exchange_name == "coinbase"
        assert health.status == "healthy"
        assert health.circuit_state == "closed"
        assert health.consecutive_failures == 0

    def test_get_health_returns_existing_record(self, dynamodb_table):
        """Test get_health returns existing health record."""
        # Insert a record
        dynamodb_table.put_item(Item={
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T00:00:00Z",
            "status": "degraded",
            "circuit_state": "half_open",
            "consecutive_failures": 2,
            "consecutive_successes": 0,
            "avg_response_time_ms": Decimal("100.0"),
            "error_rate": Decimal("0.1"),
            "request_count": 50,
            "ttl": 1704153600,
        })

        tracker = ExchangeHealthTracker(table_name="test-exchange-health")
        health = tracker.get_health("coinbase")

        assert health.status == "degraded"
        assert health.circuit_state == "half_open"
        assert health.consecutive_failures == 2

    def test_get_health_returns_latest_record(self, dynamodb_table):
        """Test get_health returns the most recent health record."""
        # Insert multiple records with different timestamps
        dynamodb_table.put_item(Item={
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T00:00:00Z",  # Older
            "status": "unhealthy",
            "circuit_state": "open",
            "consecutive_failures": 5,
            "consecutive_successes": 0,
            "avg_response_time_ms": Decimal("0"),
            "error_rate": Decimal("0.5"),
            "request_count": 10,
            "ttl": 1704153600,
        })
        dynamodb_table.put_item(Item={
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T01:00:00Z",  # Newer
            "status": "healthy",
            "circuit_state": "closed",
            "consecutive_failures": 0,
            "consecutive_successes": 5,
            "avg_response_time_ms": Decimal("50.0"),
            "error_rate": Decimal("0.01"),
            "request_count": 20,
            "ttl": 1704157200,
        })

        tracker = ExchangeHealthTracker(table_name="test-exchange-health")
        health = tracker.get_health("coinbase")

        # Should return the newer record
        assert health.status == "healthy"
        assert health.circuit_state == "closed"
        assert health.consecutive_successes == 5

    def test_update_health_creates_record(self, dynamodb_table):
        """Test update_health creates a new record."""
        tracker = ExchangeHealthTracker(table_name="test-exchange-health")

        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            status="degraded",
            circuit_state="half_open",
            consecutive_failures=3,
        )

        tracker.update_health(health)

        # Verify the record was created
        response = dynamodb_table.query(
            KeyConditionExpression="exchange_name = :exchange",
            ExpressionAttributeValues={":exchange": "coinbase"},
        )
        assert len(response["Items"]) == 1
        assert response["Items"][0]["circuit_state"] == "half_open"

    def test_update_health_sets_ttl(self, dynamodb_table):
        """Test update_health sets TTL to 7 days from now."""
        tracker = ExchangeHealthTracker(table_name="test-exchange-health")

        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )

        before_update = datetime.now(timezone.utc)
        tracker.update_health(health)
        after_update = datetime.now(timezone.utc)

        # Verify TTL is set
        response = dynamodb_table.query(
            KeyConditionExpression="exchange_name = :exchange",
            ExpressionAttributeValues={":exchange": "coinbase"},
        )

        ttl = response["Items"][0]["ttl"]
        expected_ttl_min = int((before_update + timedelta(days=7)).timestamp())
        expected_ttl_max = int((after_update + timedelta(days=7)).timestamp())
        assert expected_ttl_min <= ttl <= expected_ttl_max

    def test_health_disabled_returns_default(self):
        """Test that disabled health checks return default healthy state."""
        os.environ["HEALTH_CHECK_ENABLED"] = "false"
        try:
            tracker = ExchangeHealthTracker(table_name="nonexistent")

            health = tracker.get_health("coinbase")

            assert health.status == "healthy"
            assert health.circuit_state == "closed"
        finally:
            os.environ.pop("HEALTH_CHECK_ENABLED", None)

    def test_conditional_transition_succeeds(self, dynamodb_table):
        """Test conditional_transition succeeds when condition is met."""
        # Insert record with open circuit
        dynamodb_table.put_item(Item={
            "exchange_name": "coinbase",
            "timestamp": "2024-01-01T00:00:00Z",
            "status": "unhealthy",
            "circuit_state": "open",
            "consecutive_failures": 5,
            "consecutive_successes": 0,
            "avg_response_time_ms": Decimal("0"),
            "error_rate": Decimal("0.5"),
            "request_count": 10,
            "ttl": 1704153600,
        })

        tracker = ExchangeHealthTracker(table_name="test-exchange-health")
        result = tracker.conditional_transition("coinbase", "open", "half_open")

        assert result is True

        # Verify the transition happened
        health = tracker.get_health("coinbase")
        assert health.circuit_state == "half_open"


# ============================================================================
# CircuitBreaker tests
# ============================================================================

class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_get_wait_time_closed_circuit_returns_zero(self):
        """Test get_wait_time returns 0 when circuit is closed."""
        mock_tracker = Mock()
        mock_tracker.get_health.return_value = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="closed",
        )

        breaker = CircuitBreaker(health_tracker=mock_tracker)
        breaker.enabled = True

        wait_time = breaker.get_wait_time("coinbase", attempt=1)

        assert wait_time == 0

    def test_get_wait_time_half_open_circuit_returns_zero(self):
        """Test get_wait_time returns 0 when circuit is half_open."""
        mock_tracker = Mock()
        mock_tracker.get_health.return_value = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="half_open",
        )

        breaker = CircuitBreaker(health_tracker=mock_tracker)
        breaker.enabled = True

        wait_time = breaker.get_wait_time("coinbase", attempt=1)

        assert wait_time == 0

    def test_get_wait_time_open_circuit_in_cooldown(self):
        """Test get_wait_time returns remaining cooldown time when circuit is open."""
        mock_tracker = Mock()
        # Set last_failure_ts to 3 seconds ago (still in cooldown with 10s base)
        recent_failure = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat()
        mock_tracker.get_health.return_value = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="open",
            last_failure_ts=recent_failure,
            reopen_count=0,  # First open, 10s cooldown
        )

        breaker = CircuitBreaker(health_tracker=mock_tracker)
        breaker.enabled = True

        wait_time = breaker.get_wait_time("coinbase", attempt=1)

        # Should return approximately 7 seconds (10 - 3) with base cooldown
        assert 5 <= wait_time <= 9

    def test_get_wait_time_open_circuit_cooldown_elapsed(self):
        """Test get_wait_time triggers half_open transition after cooldown."""
        mock_tracker = Mock()
        # Set last_failure_ts to 400 seconds ago (cooldown elapsed)
        old_failure = (datetime.now(timezone.utc) - timedelta(seconds=400)).isoformat()
        mock_tracker.get_health.return_value = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="open",
            last_failure_ts=old_failure,
        )
        mock_tracker.conditional_transition.return_value = True  # Won the race

        breaker = CircuitBreaker(health_tracker=mock_tracker)
        breaker.enabled = True

        wait_time = breaker.get_wait_time("coinbase", attempt=1)

        assert wait_time == 0
        mock_tracker.conditional_transition.assert_called_once_with("coinbase", "open", "half_open")

    def test_get_wait_time_disabled_returns_zero(self):
        """Test get_wait_time returns 0 when circuit breaker is disabled."""
        mock_tracker = Mock()

        breaker = CircuitBreaker(health_tracker=mock_tracker)
        breaker.enabled = False

        wait_time = breaker.get_wait_time("coinbase", attempt=1)

        assert wait_time == 0
        mock_tracker.get_health.assert_not_called()

    def test_record_success_increments_counters(self):
        """Test record_success increments success counters."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            consecutive_failures=2,
            consecutive_successes=0,
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

        # Verify counters updated
        assert health.consecutive_successes == 1
        assert health.consecutive_failures == 0
        assert health.request_count == 1
        mock_tracker.update_health.assert_called_once()

    def test_record_success_calculates_moving_average(self):
        """Test record_success calculates exponential moving average for response time."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            avg_response_time_ms=100.0,
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=200.0)

        # EMA: 0.8 * 100 + 0.2 * 200 = 80 + 40 = 120
        assert health.avg_response_time_ms == 120.0

    def test_record_success_closes_circuit_after_threshold(self):
        """Test record_success closes circuit after SUCCESS_THRESHOLD consecutive successes."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="half_open",
            consecutive_successes=SUCCESS_THRESHOLD - 1,  # One more success will close
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

        assert health.circuit_state == "closed"
        assert health.status == "healthy"

    def test_record_success_updates_health_status(self):
        """Test record_success updates health status based on error rate."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="closed",
            status="healthy",
        )
        # Pre-populate rolling window with some failures to get degraded error rate
        health.recent_results = [True] * 85 + [False] * 14  # ~14% error rate
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

        # 14/100 = 14% > DEGRADED_ERROR_RATE (10%) but < UNHEALTHY_ERROR_RATE (30%)
        assert health.status == "degraded"

    def test_record_failure_increments_counters(self):
        """Test record_failure increments failure counters."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            consecutive_failures=0,
            consecutive_successes=5,
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_failure("coinbase", error_msg="API timeout")

        assert health.consecutive_failures == 1
        assert health.consecutive_successes == 0
        assert health.last_error_message == "API timeout"
        mock_tracker.update_health.assert_called_once()

    def test_record_failure_truncates_long_error_message(self):
        """Test record_failure truncates error messages longer than 500 chars."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            long_error = "x" * 1000
            breaker.record_failure("coinbase", error_msg=long_error)

        assert len(health.last_error_message) == 500

    def test_record_failure_opens_circuit_after_max_retries(self):
        """Test record_failure opens circuit after MAX_RETRIES consecutive failures."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="closed",
            consecutive_failures=MAX_RETRIES - 1,  # One more failure will open
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_failure("coinbase", error_msg="API error")

        assert health.circuit_state == "open"
        assert health.status == "unhealthy"

    def test_record_failure_reopens_circuit_on_half_open_failure(self):
        """Test record_failure reopens circuit if failure occurs during half_open."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
            circuit_state="half_open",
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_failure("coinbase", error_msg="Recovery test failed")

        assert health.circuit_state == "open"
        assert health.status == "unhealthy"

    def test_record_success_publishes_metrics(self):
        """Test record_success publishes CloudWatch metrics."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_metrics = Mock()
            mock_get_metrics.return_value = mock_metrics

            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

            mock_metrics.put_exchange_error_rate.assert_called_once()
            mock_metrics.put_circuit_breaker_state.assert_called_once()

    def test_record_failure_publishes_metrics(self):
        """Test record_failure publishes CloudWatch metrics."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_metrics = Mock()
            mock_get_metrics.return_value = mock_metrics

            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_failure("coinbase", error_msg="API error")

            mock_metrics.put_exchange_error_rate.assert_called_once()
            mock_metrics.put_circuit_breaker_state.assert_called_once()


# ============================================================================
# Error rate calculation tests
# ============================================================================

class TestErrorRateCalculation:
    """Tests for error rate calculation in rolling window."""

    def test_error_rate_calculation_after_success(self):
        """Test error rate is recalculated after success."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        # Pre-populate with 10 results (2 failures, 8 successes = 20% error rate)
        health.recent_results = [False, False, True, True, True, True, True, True, True, True]
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

        # After adding success: 2 failures out of 11 = ~18.2%
        assert abs(health.error_rate - 2/11) < 0.001

    def test_error_rate_calculation_after_failure(self):
        """Test error rate is recalculated after failure."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        # Pre-populate with 10 results (2 failures, 8 successes = 20% error rate)
        health.recent_results = [False, False, True, True, True, True, True, True, True, True]
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_failure("coinbase", error_msg="API error")

        # After adding failure: 3 failures out of 11 = ~27.3%
        assert abs(health.error_rate - 3/11) < 0.001

    def test_rolling_window_truncation(self):
        """Test rolling window is truncated at ROLLING_WINDOW_SIZE."""
        mock_tracker = Mock()
        health = ExchangeHealth(
            exchange_name="coinbase",
            timestamp="2024-01-01T00:00:00Z",
        )
        # Pre-populate with exactly ROLLING_WINDOW_SIZE results
        health.recent_results = [True] * ROLLING_WINDOW_SIZE
        mock_tracker.get_health.return_value = health

        with patch("schemahub.metrics.get_metrics_client") as mock_get_metrics:
            mock_get_metrics.return_value = Mock()
            breaker = CircuitBreaker(health_tracker=mock_tracker)
            breaker.record_success("coinbase", response_time_ms=100.0)

        # Should still be ROLLING_WINDOW_SIZE
        assert len(health.recent_results) == ROLLING_WINDOW_SIZE


# ============================================================================
# Global singleton tests
# ============================================================================

class TestGlobalSingletons:
    """Tests for global singleton instances."""

    def test_get_health_tracker_returns_instance(self):
        """Test get_health_tracker returns an ExchangeHealthTracker."""
        # Reset global state
        import schemahub.health as health_module
        health_module._health_tracker = None

        tracker = get_health_tracker()

        assert isinstance(tracker, ExchangeHealthTracker)

    def test_get_health_tracker_returns_same_instance(self):
        """Test get_health_tracker returns the same instance on repeated calls."""
        # Reset global state
        import schemahub.health as health_module
        health_module._health_tracker = None

        tracker1 = get_health_tracker()
        tracker2 = get_health_tracker()

        assert tracker1 is tracker2

    def test_get_circuit_breaker_returns_instance(self):
        """Test get_circuit_breaker returns a CircuitBreaker."""
        # Reset global state
        import schemahub.health as health_module
        health_module._circuit_breaker = None
        health_module._health_tracker = None

        breaker = get_circuit_breaker()

        assert isinstance(breaker, CircuitBreaker)

    def test_get_circuit_breaker_returns_same_instance(self):
        """Test get_circuit_breaker returns the same instance on repeated calls."""
        # Reset global state
        import schemahub.health as health_module
        health_module._circuit_breaker = None

        breaker1 = get_circuit_breaker()
        breaker2 = get_circuit_breaker()

        assert breaker1 is breaker2
