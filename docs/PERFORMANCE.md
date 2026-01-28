# Performance & Concurrency Model

This document covers the threading architecture, rate limiting, and performance tuning for SchemaHub's ingestion pipeline.

---

## Quick Reference

| Setting | Default | Optimal | Max |
|---------|---------|---------|-----|
| `--workers` | 3 | 5 | 10+ |
| `--chunk-concurrency` | 5 | 9 | 20 |
| **Total threads** | 15 | 45 | 200 |
| Throughput | ~8 req/s | ~15 req/s | ~15 req/s |

**TL;DR:** Use `--workers 5 --chunk-concurrency 9` for optimal throughput (45 threads). More threads than 45 won't help—the API rate limit is the bottleneck.

---

## Why Parallelism Matters

**The Problem:** Coinbase API has high latency (200ms-2s+ per request). Sequential fetching yields only 0.5-2 req/sec, wasting 80% of the rate limit headroom.

**The Solution:** Multiple threads make concurrent requests. While one thread waits for a response, others can submit requests. This achieves near-maximum throughput (8-15 req/sec) while respecting rate limits.

---

## Two-Level Threading Model

The ingestion pipeline implements **two-level parallelism**:

```
Level 1: Product Workers (--workers N)
├── BTC-USD Worker
│   └── Level 2: Chunk Threads (--chunk-concurrency M)
│       ├── Thread 1 ─→ API request
│       ├── Thread 2 ─→ API request
│       └── Thread M ─→ API request
├── ETH-USD Worker
│   └── [M chunk threads]
└── ... (N total workers)

Total threads = N × M
```

### Level 1 — Product Workers

Each product (BTC-USD, ETH-USD, etc.) gets its own worker thread. Workers run in parallel, each managing its own checkpoint and state.

### Level 2 — Chunk Concurrency

Within each product worker, multiple threads fetch chunks concurrently. A chunk is a batch of 1000 trades from a specific time range.

### Configuration Examples

```bash
# Conservative (default)
python3 -m schemahub.cli ingest --workers 3 --chunk-concurrency 5  # 15 threads

# Optimal for rate limit
python3 -m schemahub.cli ingest --workers 5 --chunk-concurrency 9  # 45 threads

# Maximum (backfill)
python3 -m schemahub.cli ingest --workers 10 --chunk-concurrency 20  # 200 threads
```

---

## Little's Law & Optimal Thread Count

Using **Little's Law**: `Optimal Concurrency = Throughput × Latency`

- Coinbase rate limit: 10-15 req/sec
- Average API latency: ~3 seconds
- **Optimal: 15 × 3 ≈ 45 threads**

```
Throughput = Concurrency / Latency

Sequential (1 thread, 500ms latency):  1 / 0.5 = 2 req/sec
Parallel (12 threads, 500ms latency): 12 / 0.5 = 24 req/sec → capped at 15 req/sec
Parallel (45 threads, 3s latency):    45 / 3.0 = 15 req/sec → saturates rate limit
```

**Below 45 threads:** Adding more improves throughput.
**At 45 threads:** You saturate the rate limit.
**Above 45 threads:** Threads just queue waiting for rate limiter tokens—no throughput gain.

---

## Performance Benchmarks

Measured on ECS Fargate (512 CPU, 1024 MB) fetching from Coinbase API:

| Configuration | Threads | API Calls (90s) | Throughput | Speedup |
|--------------|---------|-----------------|------------|---------|
| Baseline (sequential) | 1 | 286 | 2.1 req/sec | 1.0x |
| Chunk-only (1×6) | 6 | 999 | 8.2 req/sec | **3.9x** |
| Two-level (2×6) | 12 | 1086 | 8.9 req/sec | **4.2x** |
| Optimal (5×9) | 45 | ~1350 | ~15 req/sec | **7.5x** |

**Key Findings:**
- Sequential mode underutilizes the rate limit by ~80%
- Parallel mode achieves ~4-7x throughput improvement
- Diminishing returns beyond ~45 threads (rate limit becomes the bottleneck)

---

## Rate Limiting Architecture

All threads share a **global token bucket** that gates API requests:

```
┌─────────────────────────────────────────────────────────────────┐
│              GLOBAL RATE LIMITER (Token Bucket Algorithm)       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Configuration:                                                 │
│    rate  = 10 req/sec (public) or 15 req/sec (authenticated)   │
│    burst = 2 × rate = 20 or 30 tokens (max bucket capacity)    │
│    refill = rate × elapsed_seconds (continuous)                │
│                                                                 │
│  Bucket: [████████████████░░░░░░░░░░░░░░]  16/30 tokens        │
│                                                                 │
│  Thread Coordination:                                           │
│                                                                 │
│   Thread 1 ─────┐                                               │
│   Thread 2 ─────┼────► rate_limiter.acquire(tokens=1)          │
│   Thread 3 ─────┤         if tokens >= 1: proceed to API       │
│   ...           │         else: sleep(wait_time), retry        │
│   Thread 45 ────┘                                               │
│                                                                 │
│  Steady-State Throughput:                                       │
│    Sequential (1 thread):   ~2 req/sec  (API latency bound)    │
│    Parallel (45 threads):  ~15 req/sec  (rate limiter bound)   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### How Token Bucket Works

1. Bucket starts full (30 tokens for authenticated)
2. Each API request consumes 1 token
3. Tokens refill at rate of 15/sec
4. If bucket empty, thread waits until token available
5. Thread-safe via `threading.Lock`

---

## Circuit Breaker Pattern

When the API starts failing, a **circuit breaker** prevents all threads from hammering a failing endpoint:

```
            ┌───────────────┐
            │    CLOSED     │◄─────────────────────────────┐
            │   (healthy)   │                              │
            │  All requests │    3 consecutive             │
            │  proceed      │    successes                 │
            └───────┬───────┘                              │
                    │                                      │
                    │ 5 consecutive failures               │
                    ▼                                      │
            ┌───────────────┐         5 min         ┌───────────────┐
            │     OPEN      │───────cooldown───────►│  HALF_OPEN    │
            │  (unhealthy)  │                        │   (testing)   │
            │  All threads  │                        │  ONE thread   │
            │  wait 5 min   │◄───────failure────────│  tests API    │
            └───────────────┘                        └───────┬───────┘
                                                             │
                                                             │ success
                                                             └─────────┘
```

### Multithreaded Behavior Example

```
Time    Thread 1      Thread 2      Thread 3      Thread 4      Circuit
─────   ────────      ────────      ────────      ────────      ───────
0.0s    500 error                                               fail=1
0.5s                  500 error                                 fail=2
1.0s                                500 error                   fail=3
1.5s    500 error                                               fail=4
2.0s                                              500 error     fail=5 → OPEN
2.0s    [waiting...]  [waiting...]  [waiting...]  [waiting...]  OPEN
...
302.0s  [waiting...]  tests API ✓   [waiting...]  [waiting...]  HALF_OPEN
303.5s                success ✓                                 success=3→CLOSE
303.5s  [resumes]     [resumes]     [resumes]     [resumes]     CLOSED
```

### Benefits

1. **Prevents cascade failures** — Stops all threads from hammering failing APIs
2. **Coordinated recovery** — Only ONE thread tests recovery (via DynamoDB atomic write)
3. **Shared state** — All workers (even across ECS tasks) see same circuit state
4. **Immediate propagation** — When circuit closes, all threads resume

---

## Why Static Thread Pool (No Dynamic Scaling)

We use a fixed thread pool rather than dynamic scaling for three reasons:

1. **The bottleneck is external, not local.** Coinbase's rate limit (10-15 req/sec) caps throughput regardless of thread count. Dynamic scaling optimizes local resources, but our ceiling is the API, not CPU or memory.

2. **Blocked threads have negligible overhead.** Our threads spend 99%+ of their time waiting on network I/O or the rate limiter. A blocked thread consumes no CPU cycles and minimal memory (~8MB stack). 200 waiting threads ≈ 1.6GB memory, zero CPU.

3. **Simplicity wins.** Static pools are predictable and debuggable. Dynamic scaling adds complexity (scaling policies, warmup/cooldown, thundering herd risks) without improving throughput when rate-limited.

### When Thread Overhead Would Matter

Thread overhead (context switching, lock contention) only becomes significant when:
- Threads are **CPU-bound** (ours are I/O-bound, blocked 99% of the time)
- Thread counts reach **500-1000+** (we cap at 200)
- Locks are **held for milliseconds** (ours are held for microseconds)

For I/O-bound API fetching with external rate limits, thread overhead is not a practical concern.

---

## Architecture Components

### 1. Global Rate Limiter (`schemahub/rate_limiter.py`)

- Token bucket algorithm enforces 10-15 req/sec across all threads
- Thread-safe with `threading.Lock`
- Allows burst of 2x rate limit for bursty patterns

### 2. Shared Work Queue (`schemahub/parallel.py`)

- Pre-calculates cursor targets (same logic as sequential mode)
- Workers pull `(cursor, attempt)` tuples from thread-safe `queue.Queue`
- On 429 errors, cursors are re-queued for retry (up to 10 attempts)
- Results sorted by trade_id before writing (threads may complete out of order)

### 3. Per-Product Locks (`schemahub/checkpoint.py`)

- DynamoDB-based distributed locks prevent concurrent writes to same product
- Lock format: `product:coinbase:BTC-USD`
- 2-hour TTL with background renewal

### 4. Circuit Breaker (`schemahub/health.py`)

- DynamoDB-backed state shared across all workers
- Three states: CLOSED → OPEN → HALF_OPEN
- Automatic recovery testing with coordinated waiting

---

## Error Handling

### 429 Rate Limit Errors

- Detected by checking for "429" in error message
- Cursor re-queued with incremented attempt counter
- Another thread picks it up later (natural backoff)
- After 10 attempts, recorded as permanent failure

### Other Errors (5xx, network, etc.)

- Immediately recorded as permanent failure
- Entire batch fails on any permanent error
- Checkpoint NOT updated — safe to retry

### All-or-Nothing Batching

- If any cursor fails permanently (after retries), entire batch fails
- Checkpoint only updated on full success
- Safe for crash recovery — just re-run to continue

---

## Monitoring

### CloudWatch Metrics

| Metric | Description | Threshold |
|--------|-------------|-----------|
| `ExchangeResponseTime` | API latency per request | >3s warning |
| `ExchangeErrorRate` | Rate limit and other errors | >10% warning |
| `RateLimitErrors` | HTTP 429 errors | >10/min warning |
| `CircuitBreakerOpens` | Circuit breaker triggered | Any = issue |

### Log Patterns

```
[RATE_LIMITER] Initialized: rate=15.0 req/sec, burst=30 tokens
[PARALLEL] ACH-USD: Fetching 30630 pages with 6 workers
[API] ACH-USD: SUCCESS - 1000 trades in 0.45s
[PARALLEL] ACH-USD: cursor=5000 rate limited, re-queued (attempt 2/10)
[PARALLEL] ACH-USD: Fetched 30,000,000 trades in 30630 pages
```

---

## Tradeoffs & Limitations

| Tradeoff | Description |
|----------|-------------|
| **Memory** | Each thread buffers trades; memory scales with thread count |
| **Complexity** | Re-queue retry, result sorting, thread coordination |
| **All-or-nothing** | If one cursor fails permanently, entire batch must retry |
| **Rate limit ceiling** | Beyond ~45 threads, rate limit is the bottleneck |
| **429 handling** | Re-queued cursors add overhead during rate limit spikes |

### When NOT to Use Parallelism

- Fetching < 1000 trades (overhead exceeds benefit)
- Debugging (use `--chunk-concurrency 1` for sequential logs)
- Low-resource environments (reduce threads to prevent thrashing)

---

## Recommended Configurations

| Use Case | Workers | Chunks | Total Threads | Notes |
|----------|---------|--------|---------------|-------|
| Testing/Debug | 1 | 1 | 1 | Sequential, easy to debug |
| Single product | 1 | 6 | 6 | Good for targeted backfill |
| Normal ingest | 3 | 5 | 15 | Default, conservative |
| **Optimal** | 5 | 9 | 45 | Saturates rate limit |
| Maximum backfill | 10 | 20 | 200 | Extra threads just queue |

**Recommendation:** Start with defaults (`--workers 3 --chunk-concurrency 5`). For backfills, use `--workers 5 --chunk-concurrency 9`.
