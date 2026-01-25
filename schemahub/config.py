"""Configuration constants for schemahub parallelism and rate limiting.

This module defines all configuration constants for:
- Rate limiting (token bucket algorithm)
- Product-level parallelism (worker threads)
- Within-product parallelism (chunk workers)
- Lock management (DynamoDB TTLs)
- Checkpoint batching
"""

# ===== Rate Limiting =====
COINBASE_RATE_LIMIT_PUBLIC = 10.0  # req/sec (unauthenticated API)
COINBASE_RATE_LIMIT_AUTHENTICATED = 10.0  # req/sec (with API keys)
RATE_LIMITER_BURST_MULTIPLIER = 2.0  # Allow 2x burst (20 or 30 tokens)

# Auto-detect rate limit based on API key presence
def get_coinbase_rate_limit() -> float:
    """Auto-detect Coinbase rate limit based on authentication.

    Returns:
        10.0 req/sec for public API, 15.0 req/sec with API keys
    """
    import os
    has_api_key = bool(os.getenv("COINBASE_API_KEY"))
    if has_api_key:
        return COINBASE_RATE_LIMIT_AUTHENTICATED
    return COINBASE_RATE_LIMIT_PUBLIC


# ===== Product-Level Parallelism =====
DEFAULT_PRODUCT_WORKERS = 3  # Number of products to process concurrently
MAX_PRODUCT_WORKERS = 10  # Upper limit for --workers flag
MIN_PRODUCT_WORKERS = 1  # Minimum workers (sequential)


# ===== Within-Product Parallelism =====
DEFAULT_CHUNK_CONCURRENCY = 5  # Parallel chunks per product
MAX_CHUNK_CONCURRENCY = 25  # Upper limit for --chunk-concurrency flag
MIN_CHUNK_CONCURRENCY = 1  # Minimum (disables within-product parallelism)
DEFAULT_CHUNK_SIZE = 1000  # Trades per chunk (matches Coinbase API limit)


# ===== Total Thread Calculation =====
# Total threads = PRODUCT_WORKERS × CHUNK_CONCURRENCY
# Default: 3 × 5 = 15 concurrent threads
# Max: 10 × 20 = 200 concurrent threads (not recommended, rate-limited anyway)
DEFAULT_TOTAL_THREADS = DEFAULT_PRODUCT_WORKERS * DEFAULT_CHUNK_CONCURRENCY  # 15
MAX_TOTAL_THREADS = MAX_PRODUCT_WORKERS * MAX_CHUNK_CONCURRENCY  # 200


# ===== Lock Configuration =====
PRODUCT_LOCK_TTL_SECONDS = 7200  # 2 hours
PRODUCT_LOCK_RENEWAL_INTERVAL = 1800  # 30 minutes (renew lock every 30 min)
JOB_LOCK_TTL_SECONDS = 14400  # 4 hours (longer for multi-product jobs)


# ===== Checkpoint Batching =====
CHECKPOINT_BATCH_SIZE = 5000  # Update checkpoint every N trades
MIN_CHECKPOINT_INTERVAL_SECONDS = 60  # Minimum time between checkpoint writes


# ===== Metrics Configuration =====
# Top products get individual CloudWatch metrics, all others bucketed into "other"
# This reduces CloudWatch costs by limiting unique metric cardinality
TOP_PRODUCTS_FOR_METRICS = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "XRP-USD"]
METRICS_OTHER_BUCKET = "other"
METRICS_BATCH_THRESHOLD = 500  # Flush metrics buffer when this many accumulated


# ===== Performance Tuning =====
# Observed Coinbase API performance (from production CloudWatch metrics)
COINBASE_API_LATENCY_P50_MS = 2000  # 2+ seconds P50 latency
COINBASE_API_LATENCY_P95_MS = 5000  # 4-6 seconds P95 latency
COINBASE_API_LATENCY_P99_MS = 9000  # 8-10 seconds P99 latency

# Expected throughput improvements (based on Little's Law)
EXPECTED_SPEEDUP_SEQUENTIAL_TO_PRODUCT_PARALLEL = 4  # 4x with 4 workers
EXPECTED_SPEEDUP_SEQUENTIAL_TO_TWO_LEVEL = 15  # 15x with 3×5 threads


__all__ = [
    # Rate limiting
    "COINBASE_RATE_LIMIT_PUBLIC",
    "COINBASE_RATE_LIMIT_AUTHENTICATED",
    "RATE_LIMITER_BURST_MULTIPLIER",
    "get_coinbase_rate_limit",

    # Product-level parallelism
    "DEFAULT_PRODUCT_WORKERS",
    "MAX_PRODUCT_WORKERS",
    "MIN_PRODUCT_WORKERS",

    # Within-product parallelism
    "DEFAULT_CHUNK_CONCURRENCY",
    "MAX_CHUNK_CONCURRENCY",
    "MIN_CHUNK_CONCURRENCY",
    "DEFAULT_CHUNK_SIZE",

    # Thread calculations
    "DEFAULT_TOTAL_THREADS",
    "MAX_TOTAL_THREADS",

    # Lock configuration
    "PRODUCT_LOCK_TTL_SECONDS",
    "PRODUCT_LOCK_RENEWAL_INTERVAL",
    "JOB_LOCK_TTL_SECONDS",

    # Checkpoint batching
    "CHECKPOINT_BATCH_SIZE",
    "MIN_CHECKPOINT_INTERVAL_SECONDS",

    # Performance constants
    "COINBASE_API_LATENCY_P50_MS",
    "COINBASE_API_LATENCY_P95_MS",
    "COINBASE_API_LATENCY_P99_MS",
    "EXPECTED_SPEEDUP_SEQUENTIAL_TO_PRODUCT_PARALLEL",
    "EXPECTED_SPEEDUP_SEQUENTIAL_TO_TWO_LEVEL",

    # Metrics configuration
    "TOP_PRODUCTS_FOR_METRICS",
    "METRICS_OTHER_BUCKET",
    "METRICS_BATCH_THRESHOLD",
]
