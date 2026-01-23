"""Component integration tests for parallel trade fetching with rate limiting.

These tests verify that multiple components work together correctly:
- Rate limiter coordinating across parallel threads
- Parallel chunks + checkpoint integrity
- Sequential vs parallel equivalence

All tests use mocks to avoid hitting real external services (API, S3, DynamoDB).
"""
import time
import pytest
from unittest.mock import Mock, patch
from dataclasses import dataclass

from schemahub.parallel import fetch_trades_parallel
from schemahub.rate_limiter import RateLimiter, reset_rate_limiters


@dataclass
class MockTrade:
    """Mock trade for integration tests."""
    trade_id: int
    price: str = "50000.0"
    size: str = "0.1"
    time: str = "2024-01-01T00:00:00Z"
    side: str = "buy"


class TestParallelRateLimiterIntegration:
    """Test that parallel fetching respects global rate limiter."""

    def setup_method(self):
        """Reset rate limiters before each test."""
        reset_rate_limiters()

    def test_rate_limiter_coordinates_parallel_chunks(self):
        """Verify rate limiter coordinates all parallel chunks."""
        # Create a strict rate limiter (5 req/sec, burst=5)
        reset_rate_limiters()
        limiter = RateLimiter(rate_per_sec=5.0, burst=5)

        # Mock connector that acquires rate limit tokens
        connector = Mock()
        call_times = []

        def mock_fetch(*args, **kwargs):
            # Acquire rate limit token (simulating real connector behavior)
            limiter.acquire()
            call_times.append(time.time())
            # Return 100 trades for each call based on cursor
            after = kwargs.get('after', 0)
            return ([MockTrade(trade_id=i) for i in range(after, after + 100)], None)

        connector.fetch_trades_with_cursor = mock_fetch

        start = time.time()

        # Fetch with 10 parallel chunks (should hit rate limit)
        fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2000,
            chunk_concurrency=10,
            limit=100,
        )

        elapsed = time.time() - start

        # With 10 chunks and ~10 API calls total:
        # - First 5 instant (burst)
        # - Remaining 5 at 5 req/sec = 1 second
        # Should take ~1 second total (with some tolerance)
        assert elapsed >= 0.8, f"Rate limiter should enforce delay, got {elapsed:.2f}s"
        assert len(call_times) >= 10, "Should have made ~10 API calls"


class TestSequentialVsParallelEquivalence:
    """Test that sequential and parallel fetching return identical results."""

    def test_sequential_and_parallel_return_same_trades(self):
        """Verify sequential (chunk_concurrency=1) and parallel (chunk_concurrency=5) are equivalent."""
        connector = Mock()

        # Mock API that returns trades in order
        def mock_fetch(product_id, limit, after, **kwargs):
            # Return 100 trades starting from 'after'
            start = after if after else 1000
            trades = [MockTrade(trade_id=i) for i in range(start, min(start + 100, 2000))]
            next_cursor = start + 100 if start + 100 < 2000 else None
            return (trades, next_cursor)

        connector.fetch_trades_with_cursor = mock_fetch

        # Fetch sequentially (chunk_concurrency=1)
        trades_sequential, highest_seq = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2000,
            chunk_concurrency=1,
        )

        # Reset connector mock
        connector.fetch_trades_with_cursor = mock_fetch

        # Fetch in parallel (chunk_concurrency=5)
        trades_parallel, highest_par = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2000,
            chunk_concurrency=5,
        )

        # Should get same trades
        assert len(trades_sequential) == len(trades_parallel)
        assert highest_seq == highest_par

        # Verify sorted order
        seq_ids = [t.trade_id for t in trades_sequential]
        par_ids = [t.trade_id for t in trades_parallel]
        assert seq_ids == par_ids

        # Verify all sorted
        assert seq_ids == sorted(seq_ids)
        assert par_ids == sorted(par_ids)


class TestCheckpointIntegrity:
    """Test checkpoint behavior with parallel chunks."""

    def test_all_or_nothing_batch_processing(self):
        """Verify that partial success doesn't leave inconsistent state."""
        connector = Mock()

        # First attempt: chunk 3 fails
        call_count = [0]

        def mock_fetch_v1(product_id, limit, after, **kwargs):
            call_count[0] += 1
            if 1200 <= after < 1300:  # Chunk 3 range
                raise Exception("Chunk 3 failed")
            return ([MockTrade(trade_id=i) for i in range(after, after + 100)], None)

        connector.fetch_trades_with_cursor = mock_fetch_v1

        # First attempt should fail
        with pytest.raises(Exception, match="failed permanently"):
            fetch_trades_parallel(
                connector=connector,
                product_id="BTC-USD",
                cursor_start=1000,
                cursor_end=1500,
                chunk_concurrency=5,
                limit=100,
            )

        # Reset for retry
        call_count[0] = 0

        def mock_fetch_v2(product_id, limit, after, **kwargs):
            # Second attempt: all succeed
            return ([MockTrade(trade_id=i) for i in range(after, after + 100)], None)

        connector.fetch_trades_with_cursor = mock_fetch_v2

        # Retry should fetch entire range again (all-or-nothing)
        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1500,
            chunk_concurrency=5,
            limit=100,
        )

        # Should get all trades
        assert len(trades) == 500


class TestParallelPerformance:
    """Test that parallel fetching improves performance with latency."""

    def test_parallel_faster_than_sequential_with_latency(self):
        """Verify parallel is faster when API has latency."""
        connector = Mock()

        # Simulate 100ms API latency - return trades only in cursor range
        def mock_fetch_with_latency(product_id, limit, after, **kwargs):
            time.sleep(0.1)  # 100ms delay
            # Only return 100 trades starting from 'after', respecting chunk bounds
            start = after
            end = min(after + 100, 1500)  # Don't go beyond cursor_end
            if start >= end:
                return ([], None)
            return ([MockTrade(trade_id=i) for i in range(start, end)], None)

        connector.fetch_trades_with_cursor = mock_fetch_with_latency

        # Sequential (1 chunk at a time)
        start_seq = time.time()
        fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1500,  # 5 chunks × 100 trades
            chunk_concurrency=1,
            limit=100,
        )
        time_sequential = time.time() - start_seq

        # Parallel (5 chunks concurrently)
        connector.fetch_trades_with_cursor = mock_fetch_with_latency
        start_par = time.time()
        fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1500,
            chunk_concurrency=5,
            limit=100,
        )
        time_parallel = time.time() - start_par

        # Parallel should be significantly faster
        # Sequential: 5 chunks × 100ms = 500ms
        # Parallel: max(5 chunks) = 100ms
        speedup = time_sequential / time_parallel
        assert speedup >= 3.0, f"Expected 4-5x speedup, got {speedup:.2f}x"


class TestEdgeCases:
    """Test edge cases in parallel fetching."""

    def test_single_chunk_parallel_mode(self):
        """Test parallel mode with only 1 chunk (should still work)."""
        connector = Mock()
        connector.fetch_trades_with_cursor.return_value = (
            [MockTrade(trade_id=i) for i in range(1000, 1100)],
            None,
        )

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1100,
            chunk_concurrency=5,  # More workers than chunks
        )

        assert len(trades) == 100
        assert highest == 1099

    def test_more_workers_than_chunks(self):
        """Test when chunk_concurrency > number of chunks."""
        connector = Mock()

        # Return trades based on cursor, respecting chunk bounds
        def mock_fetch(product_id, limit, after, **kwargs):
            start = after
            end = min(after + 100, 1200)  # Don't exceed cursor_end
            if start >= end:
                return ([], None)
            return ([MockTrade(trade_id=i) for i in range(start, end)], None)

        connector.fetch_trades_with_cursor = mock_fetch

        # 2 chunks (1000-1100, 1100-1200), 10 workers (excess workers should be idle)
        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1200,
            chunk_concurrency=10,
            limit=100,
        )

        assert len(trades) == 200

    def test_very_small_range(self):
        """Test with very small trade range (< chunk size)."""
        connector = Mock()
        connector.fetch_trades_with_cursor.return_value = (
            [MockTrade(trade_id=i) for i in range(1000, 1010)],
            None,
        )

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1010,  # Only 10 trades
            chunk_concurrency=5,
            limit=1000,
        )

        assert len(trades) == 10
