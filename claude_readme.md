# Claude Development Notes

This document contains implementation notes, design decisions, and documentation for features added during development.

---

## Concurrency Model

### Thread Configuration

| Flag | Default | Range | Description |
|------|---------|-------|-------------|
| `--workers` | 3 | 1-10 | Products processed in parallel |
| `--chunk-concurrency` | 5 | 1-20 | Chunks fetched per product |

**Total threads = workers × chunk-concurrency** (default: 15, max: 200)

### Optimal Thread Count: ~45

Using Little's Law: **Optimal Concurrency = Throughput × Latency**

- Coinbase rate limit: 10-15 req/sec
- Average API latency: ~3 seconds
- **Optimal: 15 × 3 ≈ 45 threads**

Below 45 threads, adding more improves throughput. At 45, you saturate the rate limit. Above 45, threads just queue waiting for rate limiter tokens—no throughput gain.

**Recommended**: `--workers 5 --chunk-concurrency 9` (45 threads)

### Why Static Thread Pool (No Dynamic Scaling)

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
