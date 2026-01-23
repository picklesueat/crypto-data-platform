"""Unit tests for rate limiter (token bucket algorithm)."""
import threading
import time
import pytest

from schemahub.rate_limiter import RateLimiter, get_rate_limiter, reset_rate_limiters


class TestRateLimiter:
    """Test the RateLimiter token bucket implementation."""

    def test_init_with_default_burst(self):
        """Test rate limiter initialization with default burst."""
        limiter = RateLimiter(rate_per_sec=10.0)
        assert limiter.rate == 10.0
        assert limiter.burst == 20  # Default: 2x rate
        assert limiter.tokens == 20.0  # Start with full bucket

    def test_init_with_custom_burst(self):
        """Test rate limiter initialization with custom burst."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=30)
        assert limiter.rate == 10.0
        assert limiter.burst == 30
        assert limiter.tokens == 30.0

    def test_init_with_invalid_rate(self):
        """Test rate limiter rejects negative rate."""
        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            RateLimiter(rate_per_sec=0)

        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            RateLimiter(rate_per_sec=-5)

    def test_acquire_single_token_instant(self):
        """Test acquiring a single token from full bucket (instant)."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=20)
        start = time.time()
        result = limiter.acquire(tokens=1)
        elapsed = time.time() - start

        assert result is True
        assert elapsed < 0.1  # Should be instant
        assert limiter.tokens == 19.0  # Consumed 1 token

    def test_acquire_multiple_tokens_instant(self):
        """Test acquiring multiple tokens from full bucket (instant)."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=20)
        result = limiter.acquire(tokens=5)

        assert result is True
        assert limiter.tokens == 15.0  # Consumed 5 tokens

    def test_acquire_blocks_when_empty(self):
        """Test that acquire blocks when bucket is empty."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        # Consume all tokens
        limiter.acquire(tokens=10)
        assert limiter.tokens == 0.0

        # Next acquire should block for ~0.1 seconds (1 token / 10 tokens/sec)
        start = time.time()
        limiter.acquire(tokens=1)
        elapsed = time.time() - start

        # Should wait ~0.1s for 1 token to refill
        assert 0.08 < elapsed < 0.15, f"Expected ~0.1s wait, got {elapsed:.3f}s"

    def test_acquire_non_blocking_returns_false(self):
        """Test that acquire(block=False) returns False when tokens unavailable."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        # Consume all tokens
        limiter.acquire(tokens=10)

        # Non-blocking acquire should return False
        result = limiter.acquire(tokens=1, block=False)
        assert result is False

    def test_acquire_non_blocking_returns_true_when_available(self):
        """Test that acquire(block=False) returns True when tokens available."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        result = limiter.acquire(tokens=1, block=False)
        assert result is True

    def test_token_refill_over_time(self):
        """Test that tokens refill at constant rate over time."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        # Consume all tokens
        limiter.acquire(tokens=10)
        assert limiter.tokens == 0.0

        # Wait for 0.5 seconds (should refill 5 tokens at 10 tokens/sec)
        time.sleep(0.5)

        current_tokens = limiter.get_current_tokens()
        # Should have refilled ~5 tokens
        assert 4.5 < current_tokens < 5.5, f"Expected ~5 tokens, got {current_tokens:.2f}"

    def test_burst_cap_prevents_overflow(self):
        """Test that tokens never exceed burst capacity."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        # Wait 2 seconds (would refill 20 tokens, but capped at burst=10)
        time.sleep(2.0)

        current_tokens = limiter.get_current_tokens()
        assert current_tokens == 10.0, f"Tokens should be capped at burst=10, got {current_tokens}"

    def test_thread_safety_concurrent_acquires(self):
        """Test that rate limiter is thread-safe with concurrent acquires."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=20)
        acquired_count = [0]  # Use list for mutable int in closure
        lock = threading.Lock()

        def worker():
            for _ in range(10):
                limiter.acquire(tokens=1)
                with lock:
                    acquired_count[0] += 1

        # Start 5 threads, each acquiring 10 tokens = 50 total
        threads = [threading.Thread(target=worker) for _ in range(5)]
        start = time.time()

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        elapsed = time.time() - start

        # All 50 tokens should be acquired
        assert acquired_count[0] == 50

        # Time should be at least (50 - 20) / 10 = 3 seconds
        # (First 20 instant from burst, remaining 30 at 10 tokens/sec)
        assert elapsed >= 2.8, f"Expected >=3s for 50 tokens, got {elapsed:.2f}s"

    def test_rate_limiter_enforces_limit(self):
        """Test that rate limiter enforces rate limit over extended period."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=20)

        start = time.time()

        # Acquire 50 tokens (first 20 instant, remaining 30 throttled)
        for _ in range(50):
            limiter.acquire(tokens=1)

        elapsed = time.time() - start

        # Expected time: (50 - 20) / 10 = 3 seconds
        # Allow some tolerance for timing precision
        assert 2.8 < elapsed < 3.5, f"Expected ~3s, got {elapsed:.2f}s"

    def test_reset_clears_state(self):
        """Test that reset() restores bucket to full state."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=20)

        # Consume all tokens
        limiter.acquire(tokens=20)
        assert limiter.tokens == 0.0

        # Reset
        limiter.reset()

        # Should be back to full bucket
        assert limiter.tokens == 20.0


class TestRateLimiterSingleton:
    """Test the global rate limiter singleton factory."""

    def setup_method(self):
        """Reset global rate limiters before each test."""
        reset_rate_limiters()

    def test_get_rate_limiter_returns_singleton(self):
        """Test that get_rate_limiter returns same instance for same exchange."""
        limiter1 = get_rate_limiter("coinbase")
        limiter2 = get_rate_limiter("coinbase")

        assert limiter1 is limiter2  # Same object

    def test_get_rate_limiter_different_exchanges(self):
        """Test that different exchanges get different rate limiters."""
        limiter_coinbase = get_rate_limiter("coinbase")
        limiter_binance = get_rate_limiter("binance")

        assert limiter_coinbase is not limiter_binance

    def test_get_rate_limiter_default_coinbase(self):
        """Test that default exchange is 'coinbase'."""
        limiter_default = get_rate_limiter()
        limiter_coinbase = get_rate_limiter("coinbase")

        assert limiter_default is limiter_coinbase

    def test_reset_rate_limiters_clears_cache(self):
        """Test that reset_rate_limiters clears singleton cache."""
        limiter1 = get_rate_limiter("coinbase")
        reset_rate_limiters()
        limiter2 = get_rate_limiter("coinbase")

        assert limiter1 is not limiter2  # Different instances after reset


class TestRateLimiterEdgeCases:
    """Test edge cases and error handling."""

    def test_acquire_zero_tokens(self):
        """Test acquiring zero tokens (should be instant)."""
        limiter = RateLimiter(rate_per_sec=10.0)
        start = time.time()
        result = limiter.acquire(tokens=0)
        elapsed = time.time() - start

        assert result is True
        assert elapsed < 0.01  # Instant

    def test_acquire_more_than_burst(self):
        """Test acquiring more tokens than burst capacity."""
        limiter = RateLimiter(rate_per_sec=10.0, burst=10)

        # Acquire 20 tokens (2x burst)
        # Should wait (20 - 10) / 10 = 1 second
        start = time.time()
        limiter.acquire(tokens=20)
        elapsed = time.time() - start

        assert 0.9 < elapsed < 1.2, f"Expected ~1s, got {elapsed:.2f}s"

    def test_very_high_rate_limit(self):
        """Test rate limiter with very high rate (>1000 req/sec)."""
        limiter = RateLimiter(rate_per_sec=1000.0, burst=2000)

        start = time.time()
        for _ in range(100):
            limiter.acquire(tokens=1)
        elapsed = time.time() - start

        # 100 tokens at 1000 tokens/sec = 0.1s (all from burst)
        assert elapsed < 0.5, f"Should be very fast, got {elapsed:.2f}s"

    def test_fractional_rate_limit(self):
        """Test rate limiter with fractional rate (e.g., 0.5 req/sec)."""
        limiter = RateLimiter(rate_per_sec=0.5, burst=1)

        # Consume burst
        limiter.acquire(tokens=1)

        # Next token should take 2 seconds (1 token / 0.5 tokens/sec)
        start = time.time()
        limiter.acquire(tokens=1)
        elapsed = time.time() - start

        assert 1.8 < elapsed < 2.3, f"Expected ~2s, got {elapsed:.2f}s"
