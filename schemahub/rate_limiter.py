"""Thread-safe rate limiter using token bucket algorithm.

This module provides a global rate limiter to coordinate API requests across
all worker threads, preventing rate limit errors (429) from exchanges.

The token bucket algorithm refills tokens at a constant rate and allows
controlled bursts while maintaining an average rate limit.
"""

import threading
import time
import logging
from typing import Optional

from schemahub.config import (
    get_coinbase_rate_limit,
    RATE_LIMITER_BURST_MULTIPLIER,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe rate limiter using token bucket algorithm.

    The token bucket refills at a constant rate (e.g., 10 tokens/sec) and
    allows bursts up to the bucket capacity (e.g., 20 tokens). When a thread
    requests a token and none are available, it blocks until tokens refill.

    Attributes:
        rate: Maximum tokens per second (e.g., 10.0 for 10 req/sec)
        burst: Maximum bucket capacity (default: 2x rate)
        tokens: Current number of tokens in bucket (float)
        last_update: Last time bucket was refilled (Unix timestamp)
        lock: Threading lock for thread-safe access
    """

    def __init__(self, rate_per_sec: float, burst: Optional[int] = None):
        """Initialize rate limiter.

        Args:
            rate_per_sec: Maximum requests per second (e.g., 10.0)
            burst: Maximum burst size (default: 2x rate_per_sec)
                  Allows temporary bursts while maintaining average rate.

        Example:
            >>> limiter = RateLimiter(rate_per_sec=10.0, burst=20)
            >>> limiter.acquire()  # Blocks if no tokens available
        """
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be positive, got {rate_per_sec}")

        self.rate = rate_per_sec
        self.burst = burst if burst is not None else 1  # No bursting - steady rate only
        self.tokens = 0.0  # Start empty - no initial burst
        self.last_update = time.time()
        self.lock = threading.Lock()

        logger.info(
            f"[RATE_LIMITER] Initialized: rate={self.rate:.1f} req/sec, "
            f"burst={self.burst} tokens (interval={1000/self.rate:.0f}ms)"
        )

    def acquire(self, tokens: int = 1, block: bool = True) -> bool:
        """Acquire tokens from the bucket.

        This method blocks by default until tokens are available. It refills
        the bucket based on elapsed time since last update.

        Args:
            tokens: Number of tokens to acquire (default: 1)
            block: If True, wait until tokens available; if False, return immediately

        Returns:
            True if tokens acquired, False if not available (when block=False)

        Example:
            >>> limiter = RateLimiter(rate_per_sec=10.0)
            >>> limiter.acquire()  # Blocks until token available
            True
            >>> limiter.acquire(block=False)  # Returns immediately
            True  # or False if no tokens
        """
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update

                # Refill tokens based on elapsed time
                # tokens += rate * elapsed (e.g., 10 tokens/sec * 0.5 sec = 5 tokens)
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= tokens:
                    # Tokens available, consume them
                    self.tokens -= tokens
                    return True

                if not block:
                    # Non-blocking mode, return immediately
                    return False

                # Calculate wait time for next token
                # wait_time = tokens_needed / rate (e.g., 1 token / 10 tokens/sec = 0.1 sec)
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate

            # Sleep outside lock to allow other threads to proceed
            # This prevents lock contention while waiting
            logger.debug(
                f"[RATE_LIMITER] Thread {threading.current_thread().name}: "
                f"Waiting {wait_time:.3f}s for {tokens_needed:.2f} tokens"
            )
            time.sleep(wait_time)

    def get_current_tokens(self) -> float:
        """Get current number of tokens in bucket (for monitoring/debugging).

        Returns:
            Current token count (float)
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            current_tokens = min(self.burst, self.tokens + elapsed * self.rate)
            return current_tokens

    def reset(self) -> None:
        """Reset rate limiter to full bucket (for testing)."""
        with self.lock:
            self.tokens = float(self.burst)
            self.last_update = time.time()
            logger.debug("[RATE_LIMITER] Reset to full bucket")


# ===== Global Rate Limiter Singleton =====
# Shared across all threads/workers to coordinate API requests

_global_rate_limiters = {}  # Exchange name -> RateLimiter instance
_global_lock = threading.Lock()  # Lock for thread-safe singleton access


def get_rate_limiter(exchange: str = "coinbase") -> RateLimiter:
    """Get or create global rate limiter for an exchange.

    This function returns a singleton RateLimiter instance for the specified
    exchange. All threads share the same instance to coordinate requests.

    Args:
        exchange: Exchange name (e.g., "coinbase", "binance", "kraken")

    Returns:
        RateLimiter instance for this exchange

    Example:
        >>> limiter = get_rate_limiter("coinbase")
        >>> limiter.acquire()  # All threads use same limiter
    """
    with _global_lock:
        if exchange not in _global_rate_limiters:
            # Auto-detect rate limit for this exchange
            if exchange == "coinbase":
                rate_limit = get_coinbase_rate_limit()
            else:
                # Default to 10 req/sec for unknown exchanges
                logger.warning(
                    f"[RATE_LIMITER] Unknown exchange '{exchange}', "
                    f"defaulting to 10 req/sec"
                )
                rate_limit = 10.0

            # Create singleton instance with no bursting (steady rate only)
            _global_rate_limiters[exchange] = RateLimiter(
                rate_per_sec=rate_limit,
                burst=1,  # No bursting - dispense tokens at fixed intervals
            )

        return _global_rate_limiters[exchange]


def reset_rate_limiters() -> None:
    """Reset all global rate limiters (for testing only).

    This clears the singleton cache and resets all rate limiters.
    Should only be used in tests.
    """
    global _global_rate_limiters
    with _global_lock:
        _global_rate_limiters.clear()
    logger.warning("[RATE_LIMITER] All rate limiters reset (test mode)")


__all__ = [
    "RateLimiter",
    "get_rate_limiter",
    "reset_rate_limiters",
]
