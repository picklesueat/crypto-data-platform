# SchemaHub: Multi-Source Crypto Trades on Iceberg

## Table of Contents

- [SchemaHub Overview](#schemahub-overview)
- [System Architecture Diagrams](#system-architecture-diagrams)
- [Getting Started](#getting-started)
- [Ingestion Jobs](#ingestion-jobs)
- [Demos](#demos)
- [Project Goals](#project-goals)
- [High-Level Architecture](#high-level-architecture)
- [Detailed Architecture Diagrams](#detailed-architecture-diagrams)
- [Storage & Catalog on AWS S3](#storage--catalog-on-aws-s3)
- [Data Model](#data-model)
- [Testing](#testing)
- [MVP Scope](#mvp-scope)
- [Possible Future Extensions & Post-POC Improvements](#possible-future-extensions--post-poc-improvements)
  - [BYO-Keys Exchange Gateway](#byo-keys-exchange-gateway-hosted-product-direction)
- [Learning Path: Data Lake Performance & Storage Internals](#learning-path-data-lake-performance--storage-internals)
- [Repository Layout](#repository-layout)

# SchemaHub Overview

docu:

backfill is essentially full refresh and ingest is incremental from the last data so no gaps their still separate in documentation and idea they just use the same command aka ingest is a subset of full backfill 

**SchemaHub** is a tiny, single-developer “data platform” for normalizing messy crypto exchange trade data into a unified **Apache Iceberg** table stored in **AWS S3**.

Think of it as a lightweight, personal **Fivetran + dbt + Iceberg** stack, purpose-built for **crypto exchanges**.

These demo flows are cinematic, high-impact, and require almost nothing beyond the minimal system. They turn your real-time + historical engine into something visual, intuitive, and impressive.

---

## System Architecture Diagrams

### Diagram 1
[Mermaid Diagram – POC](https://www.mermaidchart.com/app/projects/114c17aa-ed6a-40b6-baa8-f53cd0c5a982/diagrams/b71a0205-dfa0-43f1-8507-a1d41d7f0b44/share/invite/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb2N1bWVudElEIjoiYjcxYTAyMDUtZGZhMC00M2YxLTg1MDctYTFkNDFkN2YwYjQ0IiwiYWNjZXNzIjoiRWRpdCIsImlhdCI6MTc2NTQ5Mjg4MX0.VxMj1IkddSC41wgrfmwEHtIIBb4Rc_59VJRgqMLueYo)

### Diagram 2
[Mermaid Diagram – Final](https://www.mermaidchart.com/app/projects/114c17aa-ed6a-40b6-baa8-f53cd0c5a982/diagrams/dfb2ab18-7251-47f7-a07d-9c7679bab848/share/invite/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb2N1bWVudElEIjoiZGZiMmFiMTgtNzI1MS00N2Y3LWEwN2QtOWM3Njc5YmFiODQ4IiwiYWNjZXNzIjoiRWRpdCIsImlhdCI6MTc2NTQ5Mjk3N30.KOihCf-S_TLl8OkOKWW3W6icd1pBYwzJ05ESMsJu_Lk)

---

## Getting Started

### Prerequisites


- An S3 bucket to act as the Parquet table
- An S3 bucket to act as the transformed Parquet table


### Typical Local / Dev Flow

1. Configure AWS & exchange credentials


2. Configure Glue jobs


3. Define YAML mappings


4. Run ingestion + unification


5. Query the unified table



---

## Ingestion Jobs

SchemaHub provides three main CLI commands for ingesting Coinbase trades into S3.

### Quick Start Example

Get up and running in 3 commands:

```bash
# 1. Initialize product list (one-time)
python3 -m schemahub.cli update-seed --fetch --write

# 2. Dry-run to preview backfill
python3 -m schemahub.cli backfill --s3-bucket my-bucket --dry-run

# 3. Start backfill with 4 workers
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume
```

---

### Recommended Workflows

**General best practices:**

1. **Initialize the product seed file** (one-time):
   ```bash
   python3 -m schemahub.cli update-seed --fetch --write
   ```
   This fetches all available Coinbase product IDs and saves them to `config/mappings/product_ids_seed.yaml`.

2. **Dry-run before backfill:**
   ```bash
   python3 -m schemahub.cli backfill --s3-bucket my-bucket --dry-run
   ```
   See what products and chunks would be processed without writing data.

3. **Start with local checkpoints:**
   For initial backfills, use local checkpoints (faster, simpler):
   ```bash
   python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume
   ```
   Checkpoints are stored in `state/` directory (one JSON file per product).

4. **Move to S3 checkpoints for production:**
   Once stable, store checkpoints in S3 for durability:
   ```bash
   python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume --checkpoint-s3
   ```

   
### `ingest` — Single or Seed-Based Ingestion with Watermark Tracking

**Use case:** Fresh data for specific products or quick snapshot ingestion. Ideal for **scheduled microbatches** (e.g., every 30 minutes via AWS Glue, cron, or Lambda).

**Behavior:**
- Fetches recent trades for a product.
- **By default, resumes from last watermark (checkpoint)** — tracks `last_trade_id` per product in S3.
- On each run, fetches only trades since the watermark (no duplicates).
- Writes raw JSONL to S3 with deterministic keys.
- Supports single product or entire seed file.
- Optional `--skip-checkpoint` flag to force fresh fetch from latest (ignores watermark).

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `product` | (optional) | Coinbase product ID (e.g., `BTC-USD`). If omitted, reads from seed file. |
| `--seed-path` | `config/mappings/product_ids_seed.yaml` | Path to product seed YAML. |
| `--limit` | 100 | Number of trades per request (max 100). |
| `--before` | (watermark) | Trade ID upper bound for pagination. If not set, uses watermark. |
| `--after` | (none) | Trade ID lower bound for pagination. |
| `--s3-bucket` | (required) | S3 bucket name for raw trades. |
| `--s3-prefix` | `schemahub/raw_coinbase_trades` | S3 key prefix (also stores watermarks here). |
| `--skip-checkpoint` | (false) | Ignore watermark and fetch latest trades fresh. Use only for backfill or testing. |

**Watermark (Checkpoint) Storage:**
- Location: `{s3_prefix}/ingest_checkpoints/{product_id}.json`
- Format: `{ "last_trade_id": 12345678, "last_updated": "2025-12-15T10:30:45Z" }`
- Managed automatically; no manual tracking needed.

**Examples:**

```bash
# Simple: always resumes from watermark (recommended)
python3 -m schemahub.cli ingest BTC-USD --s3-bucket my-bucket

# Ingest all products from seed file (each resumes independently)
python3 -m schemahub.cli ingest --s3-bucket my-bucket

# Force fresh fetch (skip watermark, useful for backfill or testing)
python3 -m schemahub.cli ingest BTC-USD --s3-bucket my-bucket --skip-checkpoint

# Override watermark with specific trade_id
python3 -m schemahub.cli ingest ETH-USD --s3-bucket my-bucket --before 123456789
```

**AWS Glue Integration (Recommended):**

```bash
# Glue job script: runs every 30 minutes, automatically resumes from watermark
python3 -m schemahub.cli ingest --s3-bucket my-bucket
```

Each run will:
1. Load watermark from S3 (stores last fetched trade_id per product).
2. Fetch new trades since that watermark.
3. Write to S3 with unique keys (includes product_id, timestamp, trade_id).
4. Update watermark with the latest trade_id.
5. **No duplicates** across runs (watermark prevents re-fetching).

**Benefits:**
- **Stateless**: works with ephemeral AWS Glue containers.
- **Resumable**: if job fails, next run continues from watermark.
- **Efficient**: fetches only new trades, not the same ~100 repeatedly.
- **Safe by default**: watermark prevents duplicates without extra flags.
- **Industry standard**: uses watermark pattern (Kafka, Spark, Kinesis, Flink all use this).

**How the Watermark + Time-Cutoff Design Works (Cold Start Protection):**

The ingest job uses a **hybrid watermark approach**: trade ID cursor + time-based safeguard. This prevents accidental full historical backfills on first run:

1. **On first run** (no checkpoint exists):
   - Fetch latest trades from Coinbase (newest first, descending by trade_id)
   - Stop fetching once trades are > 45 minutes old
   - Save only the recent ~30-45 min of data
   - Store `last_trade_id` in checkpoint

2. **On subsequent runs** (checkpoint exists):
   - Fetch trades after `last_trade_id` (guaranteed no duplicates)
   - As a safety net, still stop at 45-min cutoff (handles clock skew, unexpected gaps)
   - Update checkpoint with new `last_trade_id`

3. **Why this matters:**
   - Coinbase API doesn't support time-based queries, only trade_id cursors
   - Without a time guard, first run would fetch ALL historical trades (massive backfill)
   - Trade IDs are monotonically increasing, so once you hit old trades, everything older is also old
   - Early exit at time cutoff saves API calls and prevents data bloat

**Architecture insight:** This is the "watermark with time-based cutoff" pattern, used in Kafka, Spark Structured Streaming, and other streaming systems. You get:
- Idempotency via trade_id watermark (no duplicates)
- Cold-start safety via time window (no surprise backfills)
- Early-exit optimization (stop fetching once trades are old enough)

---

### `backfill` — Bulk Historical Ingestion with Resume

**Use case:** Backfill historical data for one or more products, with fault tolerance and concurrent processing.

**Behavior:**
- Reads products from seed file.
- Fetches all available trades in chunks (default 1000 per request).
- Processes multiple products concurrently via worker threads.
- Accumulates up to 1 million trades locally in memory before writing to S3 (reduces S3 API calls by ~1000x).
- Uses the `CB-AFTER` pagination header from the Coinbase API for seamless cursor-based pagination.
- Saves checkpoints after each S3 write; supports `--resume` to continue from last trade ID.
- Stores raw JSONL files in S3, organized by product and timestamp.

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `--seed-path` | `config/mappings/product_ids_seed.yaml` | Product seed file. |
| `--chunk-size` | 1000 | Trades per API request (max 100 per Coinbase API limit, but batched locally). |
| `--workers` | 1 | Concurrent product workers (e.g., 4 = process 4 products in parallel). |
| `--resume` | (false) | Load and resume from existing checkpoints. |
| `--checkpoint-s3` | (false) | Store checkpoints in S3; default is local `state/` dir. |
| `--s3-bucket` | (required) | S3 bucket for raw trades and checkpoints. |
| `--s3-prefix` | `schemahub/raw_coinbase_trades` | S3 key prefix. |
| `--dry-run` | (false) | Show plan without writing data. |

**How chunking & pagination work:**
- Coinbase API returns trades in descending order by trade ID (newest first).
- The `after` parameter is used as a cursor: "give me trades with trade_id > X".
- Trades are fetched in chunks of 1000, accumulated locally, and written to S3 once 1 million trades are cached.
- The API response includes a `CB-AFTER` header that provides the exact cursor for the next request.
- Each worker processes one product at a time, fetching chunks and caching trades locally.
- After each S3 write (every 1M trades), a checkpoint is saved with `last_trade_id`, so resuming continues from there.
- Backfill stops when Coinbase returns an empty response (all historical trades fetched).

**Examples:**

```bash
# Dry-run to see what would be backfilled
python3 -m schemahub.cli backfill --s3-bucket my-bucket --dry-run

# Start backfill with 4 concurrent workers, local checkpoints
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume

# Start backfill, store checkpoints in S3
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume --checkpoint-s3

# Resume a previously interrupted backfill
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 4 --resume --checkpoint-s3

# Larger chunks (fetches data faster, written in ~1M trade batches)
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 2 --chunk-size 1000 --resume
```

**Checkpoint format (stored as JSON):**

Local: `state/{product_id}.json`  
S3: `{s3_prefix}/checkpoints/{product_id}.json`

```json
{
  "last_trade_id": 12345678,
  "trades_processed": 1000000,
  "last_updated": "2025-12-15T10:30:45.123456Z"
}
```

**Performance notes:**
- Local caching of 1M trades uses ~500 MB of memory per backfill worker.
- Each S3 write represents 1M trades, reducing API overhead by ~1000x vs. writing per-batch.
- The `CB-AFTER` header ensures accurate pagination without manual trade ID tracking.

---

### `update-seed` — Manage Product Seed File

**Use case:** Initialize or refresh the product seed file with current Coinbase offerings.

**Behavior:**
- Fetches all available product IDs from Coinbase's `/products` endpoint.
- Optionally filters by regex (e.g., keep only USD pairs).
- Optionally merges with existing seed file.
- Writes to `config/mappings/product_ids_seed.yaml`.

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `--path` | `config/mappings/product_ids_seed.yaml` | Seed file path. |
| `--merge` | (false) | Merge fetched IDs with existing seed instead of replacing. |
| `--filter-regex` | (none) | Only keep product IDs matching this regex (e.g., `.*-USD`). |
| `--dry-run` | (false) | Show what would be written without writing. |

**Examples:**

```bash
# Fetch all products and update seed (one-time setup)
python3 -m schemahub.cli update-seed --fetch --write

# Dry-run: see what would be written
python3 -m schemahub.cli update-seed --dry-run

# Fetch only USD pairs
python3 -m schemahub.cli update-seed --fetch --write --filter-regex '.*-USD'

# Fetch and merge with existing seed (add new products, keep old ones)
python3 -m schemahub.cli update-seed --fetch --write --merge
```

---

### Miscellaneous

#### Product Seed File

**Location:** `config/mappings/product_ids_seed.yaml`

**Format:**

```yaml
product_ids:
  - BTC-USD
  - ETH-USD
  - LTC-USD
metadata:
  source: coinbase
  last_updated: 2025-12-15T12:00:00Z
  count: 3
```

The `ingest` and `backfill` commands read from this file when no explicit product is provided. Update it via `update-seed` command.

#### S3 Layout

Raw trades are organized as follows:

```
s3://{bucket}/{s3_prefix}/
  raw_coinbase_trades_{product_id}_{timestamp}_{trade_id}.jsonl
  raw_coinbase_trades_{product_id}_{timestamp}_{trade_id}.jsonl
  ...
  checkpoints/
    {product_id}.json
    {product_id}.json
    ...
```

Each JSONL file contains newline-delimited JSON trade records:

```json
{
  "trade_id": "123456",
  "product_id": "BTC-USD",
  "price": "43210.50",
  "size": "0.01",
  "time": "2025-12-15T10:30:45.123456Z",
  "side": "BUY",
  "_source": "coinbase",
  "_source_ingest_ts": "2025-12-15T10:30:50.000000Z",
  "_raw_payload": "{...}"
}
```

#### Checkpoint Recovery

If a backfill job crashes:

1. Checkpoints are already saved in `state/` (or S3 if `--checkpoint-s3`).
2. Simply re-run the same `backfill` command with `--resume`.
3. It will load checkpoints and continue from the last processed trade ID.
4. No duplicates are written (Idempotent by design).

#### Rate Limiting

Coinbase public API (no auth required) is generally rate-limited at **10 req/sec**.
Authenticated API (with API keys) allows **15 req/sec**.

The CLI implements a proactive token bucket rate limiter that coordinates all threads to stay under the limit. If you still hit rate limits, reduce `--workers` or `--chunk-concurrency`.

---

### Multithreading & Parallelism

#### Overview

The ingestion pipeline implements **two-level parallelism** to maximize throughput:

1. **Product-level parallelism** (`--workers N`): Process N products concurrently
2. **Within-product parallelism** (`--chunk-concurrency N`): Fetch N pages in parallel per product

Total threads = `workers × chunk_concurrency`

```
Example: --workers 2 --chunk-concurrency 6
         = 2 products × 6 chunks = 12 concurrent threads
         = All coordinated by global rate limiter
```

#### Why Two-Level Parallelism?

**The Problem**: Coinbase API has high latency (200ms-2s+ per request). Sequential fetching yields only 0.5-2 req/sec, wasting most of the rate limit headroom.

**The Solution**: Multiple threads make concurrent requests. While one thread waits for a response, others can submit requests. This achieves near-maximum throughput (8-10 req/sec) while respecting rate limits.

**Little's Law**: `Throughput = Concurrency / Latency`
- Sequential (1 thread, 500ms latency): 1 / 0.5 = 2 req/sec
- Parallel (12 threads, 500ms latency): 12 / 0.5 = 24 req/sec → capped at 10 req/sec

#### Performance Benchmarks

Measured on ECS Fargate (512 CPU, 1024 MB) fetching from Coinbase API:

| Configuration | Threads | API Calls (90s) | Throughput | Speedup |
|--------------|---------|-----------------|------------|---------|
| Baseline (sequential) | 1 | 286 | 2.1 req/sec | 1.0x |
| Chunk-only (1×6) | 6 | 999 | 8.2 req/sec | **3.9x** |
| Two-level (2×6) | 12 | 1086 | 8.9 req/sec | **4.2x** |

**Key Findings**:
- Sequential mode underutilizes the rate limit by ~80%
- Parallel mode achieves ~4x throughput improvement
- Diminishing returns beyond 6-8 threads (rate limit becomes the bottleneck)

#### Architecture Components

**1. Global Rate Limiter** (`schemahub/rate_limiter.py`)
- Token bucket algorithm enforces 10-15 req/sec across all threads
- Thread-safe with `threading.Lock`
- Allows burst of 2x rate limit for bursty patterns

**2. Shared Work Queue** (`schemahub/parallel.py`)
- Pre-calculates cursor targets (same logic as sequential mode)
- Workers pull `(cursor, attempt)` tuples from thread-safe `queue.Queue`
- On 429 errors, cursors are re-queued for retry (up to 10 attempts)
- Results sorted by trade_id before writing (threads may complete out of order)

**3. Per-Product Locks** (`schemahub/checkpoint.py`)
- DynamoDB-based distributed locks prevent concurrent writes to same product
- Lock format: `product:coinbase:BTC-USD`
- 2-hour TTL with background renewal

**4. All-or-Nothing Batching**
- If any cursor fails permanently (after retries), entire batch fails
- Checkpoint only updated on full success
- Safe for crash recovery - just re-run to continue

#### Configuration Flags

```bash
python3 -m schemahub.cli ingest \
    --workers 3 \              # Product-level parallelism (default: 3)
    --chunk-concurrency 5 \    # Within-product parallelism (default: 5)
    --s3-bucket my-bucket \
    --checkpoint-s3
```

**Recommended Configurations**:

| Use Case | Workers | Chunks | Total Threads |
|----------|---------|--------|---------------|
| Testing/Debug | 1 | 1 | 1 |
| Single product backfill | 1 | 6 | 6 |
| Multi-product ingest | 3 | 5 | 15 |
| Maximum throughput | 2 | 6 | 12 |

#### Tradeoffs & Limitations

| Tradeoff | Description |
|----------|-------------|
| **Memory** | Each thread buffers trades; memory scales with thread count |
| **Complexity** | Re-queue retry, result sorting, thread coordination |
| **All-or-nothing** | If one cursor fails permanently, entire batch must retry |
| **Rate limit ceiling** | Beyond ~10 threads, rate limit is the bottleneck |
| **429 handling** | Re-queued cursors add overhead during rate limit spikes |

**When NOT to use parallelism**:
- Fetching < 1000 trades (overhead exceeds benefit)
- Debugging (use `--chunk-concurrency 1` for sequential logs)
- Low-resource environments (reduce threads to prevent thrashing)

#### Error Handling

**429 Rate Limit Errors**:
- Detected by checking for "429" in error message
- Cursor re-queued with incremented attempt counter
- Another thread picks it up later (natural backoff)
- After 10 attempts, recorded as permanent failure

**Other Errors (5xx, network, etc.)**:
- Immediately recorded as permanent failure
- Entire batch fails on any permanent error
- Checkpoint NOT updated - safe to retry

#### Monitoring

**CloudWatch Metrics** (when running on ECS):
- `ExchangeResponseTime`: API latency per request
- `ExchangeErrorRate`: Rate limit and other errors
- Task logs at `/ecs/schemahub`

**Log Patterns**:
```
[RATE_LIMITER] Initialized: rate=15.0 req/sec, burst=30 tokens
[PARALLEL] ACH-USD: Fetching 30630 pages with 6 workers
[API] ACH-USD: SUCCESS - 1000 trades in 0.45s
[PARALLEL] ACH-USD: cursor=5000 rate limited, re-queued (attempt 2/10)
[PARALLEL] ACH-USD: Fetched 30,000,000 trades in 30630 pages
```

#### Engineering Decisions: Thread Pool Design

##### Optimal Thread Count: ~45

Using Little's Law: **Optimal Concurrency = Throughput × Latency**

- Coinbase rate limit: 10-15 req/sec
- Average API latency: ~3 seconds
- **Optimal: 15 × 3 ≈ 45 threads**

Below 45 threads, adding more improves throughput. At 45, you saturate the rate limit. Above 45, threads just queue waiting for rate limiter tokens—no throughput gain.

**Recommended for maximum throughput**: `--workers 5 --chunk-concurrency 9` (45 threads)

##### Why Static Thread Pool (No Dynamic Scaling)

We use a fixed thread pool rather than dynamic scaling for three reasons:

1. **The bottleneck is external, not local.** Coinbase's rate limit (10-15 req/sec) caps throughput regardless of thread count. Dynamic scaling optimizes local resources, but our ceiling is the API, not CPU or memory.

2. **Blocked threads have negligible overhead.** Our threads spend 99%+ of their time waiting on network I/O or the rate limiter. A blocked thread consumes no CPU cycles and minimal memory (~8MB stack). 200 waiting threads ≈ 1.6GB memory, zero CPU.

3. **Simplicity wins.** Static pools are predictable and debuggable. Dynamic scaling adds complexity (scaling policies, warmup/cooldown, thundering herd risks) without improving throughput when rate-limited.

##### When Thread Overhead Would Matter

Thread overhead (context switching, lock contention) only becomes significant when:
- Threads are **CPU-bound** (ours are I/O-bound, blocked 99% of the time)
- Thread counts reach **500-1000+** (we cap at 200)
- Locks are **held for milliseconds** (ours are held for microseconds)

For I/O-bound API fetching with external rate limits, thread overhead is not a practical concern.

---

## Production Dashboard

The pipeline runs continuously on **AWS ECS Fargate** with automated scheduling. The CloudWatch dashboard provides real-time visibility into pipeline operations, API health, and data quality.

### Dashboard Screenshots

#### Headline Stats
![Dashboard Header](assets/dashboard-header.png)
- **1.33 Billion** total trade records
- **29.4 GB** Parquet storage
- **11.2 years** of historical data
- **19 active products**
- **70** health score
- **327K** average daily growth

#### Pipeline Operations
![Pipeline Operations](assets/dashboard-pipeline-ops.png)
- **Ingest Jobs (24h)**: 4 successful runs
- **Trades Ingested (24h)**: 51.1 million
- **Products Processed**: 22
- **Lambda Invocations**: 6
- **Lambda Errors**: 0

#### API Health & Reliability
![API Health](assets/dashboard-api-health.png)
- **API Success (24h)**: 140K requests
- **Rate Limit Errors**: 416 (managed by token bucket)
- **Server Errors**: 6
- **Timeout Errors**: 93
- **Circuit Breaker State**: Closed (healthy)
- **Avg Response Time**: 1.8 sec

#### Data Completeness & Storage
![Data Completeness](assets/dashboard-data-completeness.png)
- **Data Completeness**: 100% (no gaps in trade ID sequences)
- **Missing Trades**: 0
- **Total Records Growth**: Trending up to 1.39B
- **Parquet Size**: Growing to ~29.6 GB

---

## Project Goals

- Ingest trade data from multiple crypto exchanges (e.g. **Binance**, **Coinbase**, **Kraken**).
- Land each source into its own **raw Iceberg table in S3** with minimal cleanup.
- Normalize everything into a **single, unified Iceberg table** with:
  - Stable schema  
  - Deduplicated trades  
  - Consistent types & naming

Target unified table: `trades_unified`

```text
exchange      STRING    -- BINANCE, COINBASE, KRAKEN, ...
symbol        STRING    -- BTCUSDT, ETH-USD, etc.
trade_id      STRING
side          STRING    -- BUY / SELL (or maker/taker depending on modeling)
price         DOUBLE
quantity      DOUBLE
trade_ts      TIMESTAMP -- when the trade executed
ingest_ts     TIMESTAMP -- when this record was written into unified table
```

---

## High-Level Architecture

SchemaHub is split into 5 logical components:

- Source Connectors
- Raw Iceberg Tables
- Schema Mapping Registry
- Unifier / Transformer
- Coordinator / CLI

### 1. Source Connectors

Each connector knows how to fetch and parse data from a specific exchange:

- Binance REST API (trades, optionally order book snapshots)
- Coinbase API (trades)
- Kraken API (trades)

(Extensible to more exchanges later)

A connector:

- Handles auth, pagination, rate limiting, and time-range queries.
- Outputs a stream / iterator of parsed JSON-like records in the source’s native schema.

Example: Binance trade record (connector output)

```json
{
  "id": "1234",
  "symbol": "BTCUSDT",
  "price": "43210.5",
  "qty": "0.01",
  "time": 1712345678
}
```

Connectors hide all API weirdness from the rest of the system.

### 2. Raw Iceberg Tables

Each source lands into its own append-only Iceberg table in S3:

- `raw_binance_trades`
- `raw_coinbase_trades`
- `raw_kraken_trades`

These tables:

- Apply only minimal transformations:
  - Cast numeric strings → DOUBLE
  - Convert epoch timestamps → TIMESTAMP
- Add ingestion metadata columns like:

```text
_source           STRING   -- e.g. 'binance'
_source_ingest_ts TIMESTAMP  -- when the row landed in raw table
_raw_payload      STRING   -- optional: original JSON payload
```

Rules:

- No updates/deletes in raw tables; only appends.
- Let Iceberg handle partitioning (e.g., by date(trade_ts) initially).

### 3. Schema Mapping Registry

Mappings describe how to go from raw tables → unified schema.

You can implement this as:

- YAML files in `config/mappings/`, or
- An Iceberg table, e.g. `iceberg_schema_mappings`.

For this project, we assume YAML files.

Example: Binance → trades_unified

```yaml
# config/mappings/binance_trades.yaml
source: raw_binance_trades
target_table: trades_unified

mappings:
  id: trade_id
  symbol: symbol
  price: price
  qty: quantity
  time: trade_ts

constants:
  exchange: BINANCE

transforms:
  trade_ts: "from_ms"       # ms epoch → TIMESTAMP
  price: "to_double"
  qty: "to_double"
```

Example: Coinbase → trades_unified

```yaml
# config/mappings/coinbase_trades.yaml
source: raw_coinbase_trades
target_table: trades_unified

mappings:
  trade_id: trade_id
  product_id: symbol
  price: price
  size: quantity
  time: trade_ts

constants:
  exchange: COINBASE

transforms:
  trade_ts: "iso8601"
  price: "to_double"
  quantity: "to_double"
```

Transform functions (like `from_ms`, `iso8601`, `to_double`) are implemented centrally in the unifier.

### 4. Unifier / Transformer

The unifier is the engine that:

- Reads new data from `raw_*` tables (since last watermark).
- Applies the mapping config for each source.
- Writes normalized rows into `trades_unified`.

Responsibilities:

- Column renaming (e.g., `qty` → `quantity`).
- Type casting (string → double, epoch → timestamp).
- Constants & defaults (exchange = 'BINANCE', set side if missing).

Optional enrichment (later):

- Parse symbol into `base_asset`, `quote_asset`.
- Map exchange-specific symbols to a canonical symbol set.

Output rows share the same `trades_unified` schema regardless of source.

### 5. Coordinator / CLI

A tiny CLI wraps everything:

```bash
# Ingest the latest trades from Binance into raw_binance_trades
schemahub run ingest binance

# Ingest from multiple sources
schemahub run ingest binance coinbase kraken

# Unify all new data into trades_unified
schemahub run unify trades_unified

# Or a combined pipeline step
schemahub run pipeline trades_unified
```

Coordinator tasks:

- Track per-source high watermarks:
  - E.g., last `_source_ingest_ts` or `last_trade_id`.
  - Stored in a meta table like `schemahub_watermarks`.
- Ensure idempotent behavior:
  - Re-running a command should not double-insert trades.
- Optionally perform an atomic Iceberg commit:
  - Ingest → transform → write unified → single snapshot.

**Production Deployment:**
The pipeline runs on **AWS ECS Fargate** with **EventBridge Scheduler** for automated execution. Logs stream to **CloudWatch**. Credentials are stored in **AWS Secrets Manager**. CloudWatch alarms monitor job health and data quality.

### Automated Job Schedules

| Schedule | Frequency | Description |
|----------|-----------|-------------|
| `schemahub-ingest-schedule` | Every 3 hours | Fetches new trades from Coinbase API for all coins in seed file |
| `schemahub-transform-schedule` | Every 3 hours | Transforms raw JSONL → curated Parquet (deduped, typed) |
| `schemahub-data-quality-schedule` | Every 6 hours | Runs Athena queries, publishes CloudWatch metrics, calculates health score |

**Enable/Disable Scheduling:**
```hcl
# terraform/terraform.tfvars
enable_scheduling = true   # Enable all schedules
enable_scheduling = false  # Manual runs only
```

**Check Schedule Status:**
```bash
aws scheduler list-schedules --query 'Schedules[].{Name:Name,State:State}'
```

**Coins in Rotation (updated 2026-01-27):**
- SOL-USD, MATIC-USD, XRP-USD, ADA-USD, AVAX-USD, DOT-USD
- AAVE-USD, ACH-USD, ABT-USD, WLD-USD, W-USD, PYTH-USD

---

## Detailed Architecture Diagrams

### End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION PHASE                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐                 │
│  │  Coinbase    │    │  CoinbaseConnector  │    │  Global Rate    │                 │
│  │  REST API    │◄───┤  (coinbase.py)  │◄───┤  Limiter        │                 │
│  │  /products/  │    │                 │    │  (rate_limiter.py)│                 │
│  │  trades      │    │  • fetch_trades │    │                 │                 │
│  └──────────────┘    │  • pagination   │    │  • 10-15 req/sec│                 │
│        │             │  • retry logic  │    │  • token bucket │                 │
│        │             └────────┬────────┘    │  • burst: 2x    │                 │
│        │                      │             └────────┬────────┘                 │
│        │                      │                      │                          │
│        ▼                      ▼                      │                          │
│  ┌──────────────────────────────────────────────────┴──────────────────────┐   │
│  │                     Parallel Fetcher (parallel.py)                       │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │   │
│  │  │  Level 1: Product Workers (--workers 3)                         │    │   │
│  │  │  ┌─────────┐   ┌─────────┐   ┌─────────┐                        │    │   │
│  │  │  │ BTC-USD │   │ ETH-USD │   │ SOL-USD │  ... (N products)      │    │   │
│  │  │  └────┬────┘   └────┬────┘   └────┬────┘                        │    │   │
│  │  │       │             │             │                             │    │   │
│  │  │  ┌────▼─────────────▼─────────────▼────┐                        │    │   │
│  │  │  │  Level 2: Chunk Workers (--chunk-concurrency 5)              │    │   │
│  │  │  │  ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐                              │    │   │
│  │  │  │  │C1 │ │C2 │ │C3 │ │C4 │ │C5 │  (5 chunks per product)     │    │   │
│  │  │  │  └───┘ └───┘ └───┘ └───┘ └───┘                              │    │   │
│  │  │  └─────────────────────────────────────┘                        │    │   │
│  │  └─────────────────────────────────────────────────────────────────┘    │   │
│  │  Total threads: 3 × 5 = 15 concurrent API requests                      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        Raw Writer (raw_writer.py)                        │   │
│  │  • Accumulates trades in memory (100K batch)                            │   │
│  │  • Writes JSONL to S3 with deterministic keys                           │   │
│  │  • Key format: raw_coinbase_trades_{product}_{ts}_{first}_{last}.jsonl  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                              AWS S3 Bucket                               │   │
│  │  s3://schemahub-crypto-{account}/                                       │   │
│  │    └── schemahub/                                                        │   │
│  │        └── raw_coinbase_trades/                                          │   │
│  │            ├── raw_coinbase_trades_BTC-USD_20250122T...jsonl            │   │
│  │            ├── raw_coinbase_trades_ETH-USD_20250122T...jsonl            │   │
│  │            └── checkpoints/                                              │   │
│  │                ├── BTC-USD.json  {"cursor": 12345678}                   │   │
│  │                └── ETH-USD.json  {"cursor": 87654321}                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TRANSFORMATION PHASE                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                     Transform Pipeline (transform.py)                    │   │
│  │                                                                          │   │
│  │  1. List raw .jsonl files from S3 (paginated)                           │   │
│  │                    │                                                     │   │
│  │                    ▼                                                     │   │
│  │  2. Filter: skip files already in manifest                              │   │
│  │                    │                                                     │   │
│  │                    ▼                                                     │   │
│  │  3. Stream & Transform:                                                  │   │
│  │     ┌────────────────────────────────────────────────────────┐          │   │
│  │     │  Raw Record              →     Unified Record          │          │   │
│  │     │  {                              {                       │          │   │
│  │     │    "trade_id": "123",            "exchange": "COINBASE",│          │   │
│  │     │    "product_id": "BTC-USD",      "symbol": "BTC-USD",   │          │   │
│  │     │    "price": "43210.50",   →      "trade_id": "123",     │          │   │
│  │     │    "size": "0.01",               "price": 43210.50,     │          │   │
│  │     │    "time": "2025-01-22...",      "quantity": 0.01,      │          │   │
│  │     │    "side": "BUY"                 "trade_ts": timestamp, │          │   │
│  │     │  }                               "side": "BUY"          │          │   │
│  │     │                                }                        │          │   │
│  │     └────────────────────────────────────────────────────────┘          │   │
│  │                    │                                                     │   │
│  │                    ▼                                                     │   │
│  │  4. Deduplicate within batch (by trade_id)                              │   │
│  │                    │                                                     │   │
│  │                    ▼                                                     │   │
│  │  5. Write Parquet to S3 (unified_coinbase_trades/)                      │   │
│  │                    │                                                     │   │
│  │                    ▼                                                     │   │
│  │  6. Update manifest.json (track processed files)                        │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                              AWS S3 Bucket                               │   │
│  │  s3://schemahub-crypto-{account}/                                       │   │
│  │    └── schemahub/                                                        │   │
│  │        └── unified_coinbase_trades/                                      │   │
│  │            ├── unified_trades_v1_20250122T120000Z.parquet               │   │
│  │            ├── unified_trades_v1_20250122T180000Z.parquet               │   │
│  │            └── manifest.json                                             │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            OBSERVABILITY LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐           │
│  │  CloudWatch       │  │  CloudWatch Logs  │  │  Data Quality     │           │
│  │  Metrics          │  │                   │  │  Lambda           │           │
│  │  (metrics.py)     │  │  /ecs/schemahub   │  │  (every 6 hours)  │           │
│  │                   │  │  /aws/lambda/...  │  │                   │           │
│  │  • IngestCount    │  │                   │  │  • Health score   │           │
│  │  • TransformRecs  │  │  • API calls      │  │  • Duplicate check│           │
│  │  • DataAgeMinutes │  │  • Errors/retries │  │  • Freshness      │           │
│  │  • ErrorRate      │  │  • Progress bars  │  │  • Stale products │           │
│  └───────────────────┘  └───────────────────┘  └───────────────────┘           │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### Rate Limiting & Thread Coordination

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    GLOBAL RATE LIMITER (Token Bucket Algorithm)                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Configuration:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  rate     = 10 req/sec (public) or 15 req/sec (authenticated)          │   │
│  │  burst    = 2 × rate = 20 or 30 tokens (max bucket capacity)           │   │
│  │  refill   = rate × elapsed_seconds (continuous)                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Token Bucket State:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │    Bucket: [████████████████░░░░░░░░░░░░░░]  16/30 tokens              │   │
│  │                                                                          │   │
│  │    Refill rate: +15 tokens/second                                       │   │
│  │    Last refill: 2025-01-22T12:00:00.123Z                                │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Thread Coordination Flow:                                                       │
│                                                                                  │
│   Thread 1 ─────┐                                                               │
│   (BTC-USD)     │     ┌─────────────────────────────────────────┐              │
│                 │     │                                          │              │
│   Thread 2 ─────┼────►│  rate_limiter.acquire(tokens=1)         │              │
│   (ETH-USD)     │     │                                          │              │
│                 │     │  with lock:                              │              │
│   Thread 3 ─────┤     │    refill_tokens()                       │              │
│   (SOL-USD)     │     │    if tokens >= 1:                       │              │
│                 │     │      tokens -= 1                         │              │
│   ...           │     │      return immediately ──────────────────┼───► API call│
│                 │     │    else:                                 │              │
│   Thread 15 ────┘     │      wait_time = (1 - tokens) / rate     │              │
│                       │      release lock                        │              │
│                       │      sleep(wait_time)                    │              │
│                       │      retry acquire ──────────────────────┼───► API call│
│                       │                                          │              │
│                       └─────────────────────────────────────────┘              │
│                                                                                  │
│  Timeline Example (15 threads, 15 req/sec rate):                                │
│                                                                                  │
│  t=0.000s  │ T1 acquires │ T2 acquires │ T3 acquires │ ... │ T15 acquires      │
│  t=0.000s  │ 15 tokens consumed instantly (burst allows this)                  │
│  t=0.067s  │ T1 done, acquires again (1 token refilled)                        │
│  t=0.133s  │ T2 done, acquires again (1 token refilled)                        │
│  ...       │ (steady state: ~15 requests/second)                               │
│                                                                                  │
│  Steady-State Throughput:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Sequential (1 thread):   ~2 req/sec  (limited by API latency)         │   │
│  │  Parallel (15 threads):  ~15 req/sec  (limited by rate limiter)        │   │
│  │  Speedup:                  7.5x                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### Checkpointing & Distributed Locking

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     CHECKPOINTING & DISTRIBUTED LOCKING                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  CHECKPOINT FLOW (per product):                                                  │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  1. LOAD CHECKPOINT                                                      │   │
│  │     ┌────────────────────┐                                               │   │
│  │     │ S3 or Local File   │                                               │   │
│  │     │ BTC-USD.json       │ ──► cursor = 12345678                        │   │
│  │     │ {                  │     (or default 1000 if not exists)          │   │
│  │     │   "cursor": 12345678│                                               │   │
│  │     │ }                  │                                               │   │
│  │     └────────────────────┘                                               │   │
│  │              │                                                           │   │
│  │              ▼                                                           │   │
│  │  2. FETCH TRADES (cursor → latest)                                       │   │
│  │     ┌────────────────────────────────────────────────────────┐          │   │
│  │     │  Coinbase API: after=12345678                          │          │   │
│  │     │  Returns: trades 12345679, 12345680, ... , 12350000    │          │   │
│  │     └────────────────────────────────────────────────────────┘          │   │
│  │              │                                                           │   │
│  │              ▼                                                           │   │
│  │  3. WRITE TO S3 (atomic)                                                 │   │
│  │     ┌────────────────────────────────────────────────────────┐          │   │
│  │     │  s3://bucket/raw_coinbase_trades_BTC-USD_...jsonl      │          │   │
│  │     │  (contains trades 12345679 - 12350000)                 │          │   │
│  │     └────────────────────────────────────────────────────────┘          │   │
│  │              │                                                           │   │
│  │              ▼                                                           │   │
│  │  4. SAVE CHECKPOINT (atomic, ONLY after successful write)                │   │
│  │     ┌────────────────────┐                                               │   │
│  │     │ BTC-USD.json       │                                               │   │
│  │     │ {                  │                                               │   │
│  │     │   "cursor": 12350001  ◄── highest_trade_id + 1                    │   │
│  │     │ }                  │                                               │   │
│  │     └────────────────────┘                                               │   │
│  │                                                                          │   │
│  │  CRASH RECOVERY: If crash before step 4, checkpoint unchanged.          │   │
│  │                  Re-run fetches same trades → idempotent S3 write.      │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  DISTRIBUTED LOCKING (DynamoDB):                                                 │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Purpose: Prevent concurrent writes to same product across ECS tasks    │   │
│  │                                                                          │   │
│  │  DynamoDB Table: schemahub-locks                                        │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │ lock_name                    │ lock_id      │ ttl        │ ...  │   │   │
│  │  ├─────────────────────────────────────────────────────────────────┤   │   │
│  │  │ product:coinbase:BTC-USD     │ uuid-1234    │ 1705936800 │      │   │   │
│  │  │ product:coinbase:ETH-USD     │ uuid-5678    │ 1705936800 │      │   │   │
│  │  │ job:ingest                   │ uuid-9999    │ 1705943800 │      │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                          │   │
│  │  Lock Lifecycle:                                                         │   │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐          │   │
│  │  │ ACQUIRE  │───►│  HOLD    │───►│ RENEW    │───►│ RELEASE  │          │   │
│  │  │(cond.put)│    │ (2 hrs)  │    │(every 30m)│   │ (delete) │          │   │
│  │  └──────────┘    └──────────┘    └──────────┘    └──────────┘          │   │
│  │       │                                                │                │   │
│  │       │ fails if exists                                │ or TTL expires │   │
│  │       ▼                                                ▼                │   │
│  │  ┌──────────┐                                    ┌──────────┐          │   │
│  │  │  WAIT    │                                    │ STEALABLE│          │   │
│  │  │ (retry)  │                                    │ (by other)│          │   │
│  │  └──────────┘                                    └──────────┘          │   │
│  │                                                                          │   │
│  │  Concurrent Access Pattern:                                              │   │
│  │                                                                          │   │
│  │    ECS Task A                      ECS Task B                           │   │
│  │    ──────────                      ──────────                           │   │
│  │    acquire(BTC-USD) ✓              acquire(BTC-USD) ✗ (already held)   │   │
│  │    process BTC-USD                 acquire(ETH-USD) ✓                   │   │
│  │    release(BTC-USD)                process ETH-USD                      │   │
│  │    acquire(SOL-USD) ✓              release(ETH-USD)                     │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### Circuit Breaker State Machine

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      CIRCUIT BREAKER STATE MACHINE                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  States & Transitions:                                                           │
│                                                                                  │
│                    ┌────────────────────────────────────────┐                   │
│                    │                                        │                   │
│                    ▼                                        │                   │
│            ┌───────────────┐                                │                   │
│            │    CLOSED     │◄───────────────────────────────┤                   │
│            │   (healthy)   │                                │                   │
│            │               │    3 consecutive               │                   │
│            │  All requests │    successes                   │                   │
│            │  proceed      │                                │                   │
│            └───────┬───────┘                                │                   │
│                    │                                        │                   │
│                    │ 5 consecutive                          │                   │
│                    │ failures                               │                   │
│                    │                                        │                   │
│                    ▼                                        │                   │
│            ┌───────────────┐         5 min         ┌───────────────┐           │
│            │     OPEN      │───────cooldown───────►│  HALF_OPEN    │           │
│            │  (unhealthy)  │                        │   (testing)   │           │
│            │               │                        │               │           │
│            │  All threads  │                        │  ONE thread   │           │
│            │  wait 5 min   │◄───────failure────────│  tests API    │           │
│            └───────────────┘                        └───────┬───────┘           │
│                                                              │                   │
│                                                              │ success           │
│                                                              │                   │
│                                                              └───────────────────┘
│                                                                                  │
│  DynamoDB Health State:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Table: schemahub-exchange-health                                       │   │
│  │  ┌───────────────┬───────────────┬─────────────┬───────────────────┐   │   │
│  │  │ exchange_name │ circuit_state │ consec_fail │ last_failure_ts   │   │   │
│  │  ├───────────────┼───────────────┼─────────────┼───────────────────┤   │   │
│  │  │ coinbase      │ closed        │ 0           │ 2025-01-22T10:00  │   │   │
│  │  └───────────────┴───────────────┴─────────────┴───────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Multithreaded Behavior (4 workers hitting 500 errors):                         │
│                                                                                  │
│  Time    Thread 1      Thread 2      Thread 3      Thread 4      Circuit        │
│  ─────   ────────      ────────      ────────      ────────      ───────        │
│  0.0s    500 error                                               fail=1         │
│  0.5s                  500 error                                 fail=2         │
│  1.0s                                500 error                   fail=3         │
│  1.5s    500 error                                               fail=4         │
│  2.0s                                              500 error     fail=5 → OPEN  │
│  2.0s    [waiting...]  [waiting...]  [waiting...]  [waiting...]  OPEN           │
│  ...                                                                            │
│  302.0s  [waiting...]  tests API ✓   [waiting...]  [waiting...]  HALF_OPEN      │
│  302.5s                success ✓                                 success=1      │
│  303.0s                success ✓                                 success=2      │
│  303.5s                success ✓                                 success=3→CLOSE│
│  303.5s  [resumes]     [resumes]     [resumes]     [resumes]     CLOSED         │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### Production Infrastructure

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AWS PRODUCTION INFRASTRUCTURE                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                           COMPUTE LAYER                                    │  │
│  │                                                                            │  │
│  │   ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐   │  │
│  │   │  EventBridge    │      │  ECS Fargate    │      │  Lambda         │   │  │
│  │   │  Scheduler      │─────►│  Cluster        │      │  Data Quality   │   │  │
│  │   │                 │      │                 │      │                 │   │  │
│  │   │  • Ingest: 45m  │      │  ┌───────────┐  │      │  • Health score │   │  │
│  │   │  • Transform:   │      │  │ Ingest    │  │      │  • Duplicates   │   │  │
│  │   │    hourly       │      │  │ Task      │  │      │  • Freshness    │   │  │
│  │   │                 │      │  │ 512 CPU   │  │      │                 │   │  │
│  │   └─────────────────┘      │  │ 1024 MB   │  │      │  Trigger: 6hr   │   │  │
│  │                            │  └───────────┘  │      └─────────────────┘   │  │
│  │                            │  ┌───────────┐  │                            │  │
│  │                            │  │ Transform │  │                            │  │
│  │                            │  │ Task      │  │                            │  │
│  │                            │  └───────────┘  │                            │  │
│  │                            └─────────────────┘                            │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                         │                                        │
│                                         ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                           STORAGE LAYER                                    │  │
│  │                                                                            │  │
│  │   ┌─────────────────────────────────────────────────────────────────────┐ │  │
│  │   │                              S3 Bucket                               │ │  │
│  │   │   schemahub-crypto-{account-id}                                     │ │  │
│  │   │                                                                      │ │  │
│  │   │   ├── schemahub/                                                     │ │  │
│  │   │   │   ├── raw_coinbase_trades/     ← JSONL files (~50MB each)       │ │  │
│  │   │   │   │   ├── raw_coinbase_trades_BTC-USD_*.jsonl                   │ │  │
│  │   │   │   │   └── checkpoints/                                          │ │  │
│  │   │   │   │       └── *.json           ← Cursor state per product       │ │  │
│  │   │   │   │                                                              │ │  │
│  │   │   │   └── unified_coinbase_trades/ ← Parquet files (columnar)       │ │  │
│  │   │   │       ├── unified_trades_v1_*.parquet                           │ │  │
│  │   │   │       └── manifest.json        ← Processed file tracking        │ │  │
│  │   │   │                                                                  │ │  │
│  │   │   └── curated_trades/              ← Athena CTAS output (dedupe)    │ │  │
│  │   │       └── *.parquet                                                  │ │  │
│  │   └─────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                            │  │
│  │   ┌─────────────────┐      ┌─────────────────┐                            │  │
│  │   │  DynamoDB       │      │  DynamoDB       │                            │  │
│  │   │  schemahub-locks│      │  exchange-health│                            │  │
│  │   │                 │      │                 │                            │  │
│  │   │  • Product locks│      │  • Circuit state│                            │  │
│  │   │  • Job locks    │      │  • Error counts │                            │  │
│  │   │  • TTL: 2-6 hrs │      │  • Response time│                            │  │
│  │   └─────────────────┘      └─────────────────┘                            │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                         │                                        │
│                                         ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                         OBSERVABILITY LAYER                                │  │
│  │                                                                            │  │
│  │   ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐   │  │
│  │   │  CloudWatch     │      │  CloudWatch     │      │  CloudWatch     │   │  │
│  │   │  Logs           │      │  Metrics        │      │  Dashboard      │   │  │
│  │   │                 │      │                 │      │                 │   │  │
│  │   │  • /ecs/schema  │      │  • IngestCount  │      │  schemahub-     │   │  │
│  │   │  • /aws/lambda  │      │  • ErrorRate    │      │  data-quality   │   │  │
│  │   │                 │      │  • DataAge      │      │                 │   │  │
│  │   │  • API calls    │      │  • CircuitState │      │  • Health gauge │   │  │
│  │   │  • Progress     │      │  • Throughput   │      │  • Record count │   │  │
│  │   │  • Errors       │      │                 │      │  • Dup trends   │   │  │
│  │   └─────────────────┘      └─────────────────┘      └─────────────────┘   │  │
│  │                                                                            │  │
│  │   ┌─────────────────┐                                                     │  │
│  │   │  CloudWatch     │                                                     │  │
│  │   │  Alarms         │                                                     │  │
│  │   │                 │                                                     │  │
│  │   │  • Task failure │                                                     │  │
│  │   │  • Stale data   │                                                     │  │
│  │   │  • High errors  │                                                     │  │
│  │   └─────────────────┘                                                     │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                         │                                        │
│                                         ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                           QUERY LAYER                                      │  │
│  │                                                                            │  │
│  │   ┌─────────────────┐      ┌─────────────────┐                            │  │
│  │   │  Amazon Athena  │      │  Glue Catalog   │                            │  │
│  │   │                 │      │                 │                            │  │
│  │   │  • Ad-hoc SQL   │◄────►│  • Table defs   │                            │  │
│  │   │  • CTAS dedupe  │      │  • Partitions   │                            │  │
│  │   │  • Analytics    │      │  • Schemas      │                            │  │
│  │   └─────────────────┘      └─────────────────┘                            │  │
│  │                                                                            │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Storage & Catalog on AWS S3

All Iceberg tables in this project are stored in Amazon S3. You can use any Iceberg-supported catalog; a common setup is AWS Glue Data Catalog + Spark.

### S3 Warehouse Layout

Create an S3 bucket (once), for example:

```text
s3://schemahub-warehouse-<your-id>/warehouse/
```

Iceberg will store tables under that warehouse path:

```text
s3://schemahub-warehouse-<your-id>/warehouse/<database>/<table>/
```

You don’t write to these paths directly; you interact through Iceberg (SQL or APIs).

### Example: Spark + AWS Glue Catalog

Configure Spark (local, EMR, or Glue job) to use Iceberg with S3 + Glue:

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.schemahub=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.schemahub.warehouse=s3://schemahub-warehouse-<your-id>/warehouse/
spark.sql.catalog.schemahub.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.schemahub.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Where:

- `schemahub` is the Iceberg catalog name.
- The Glue Data Catalog stores table metadata; data files live in S3.
- Create a Glue database (e.g. `schemahub`) that acts as the Iceberg namespace.

### Table Naming Convention

With the config above, tables are addressed as:

```text
schemahub.<database>.<table>
```

For example:

- `schemahub.schemahub.raw_binance_trades`
- `schemahub.schemahub.raw_coinbase_trades`
- `schemahub.schemahub.trades_unified`

(First `schemahub` = catalog, second `schemahub` = database/namespace.)

### Example DDL for S3 Iceberg Tables

Raw Binance trades table:

```sql
CREATE TABLE IF NOT EXISTS schemahub.schemahub.raw_binance_trades (
  id                STRING,
  symbol            STRING,
  price             DOUBLE,
  qty               DOUBLE,
  time              TIMESTAMP,
  _source           STRING,
  _source_ingest_ts TIMESTAMP,
  _raw_payload      STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2');
```

Unified trades table:

```sql
CREATE TABLE IF NOT EXISTS schemahub.schemahub.trades_unified (
  exchange   STRING,
  symbol     STRING,
  trade_id   STRING,
  side       STRING,
  price      DOUBLE,
  quantity   DOUBLE,
  trade_ts   TIMESTAMP,
  ingest_ts  TIMESTAMP
)
USING iceberg
PARTITIONED BY (date(trade_ts))
TBLPROPERTIES ('format-version'='2');
```

### How the Pipeline Uses S3

Connectors write new data into:

- `schemahub.schemahub.raw_binance_trades`
- `schemahub.schemahub.raw_coinbase_trades`
- `schemahub.schemahub.raw_kraken_trades`

The unifier reads from these raw tables and writes normalized data to:

- `schemahub.schemahub.trades_unified`

All of this happens through Iceberg; you get ACID semantics, schema evolution, and time travel while the actual data lives in S3.

---

## Data Model

### Raw Tables

Example (Binance):

```text
raw_binance_trades
-------------------------
id                STRING
symbol            STRING
price             DOUBLE
qty               DOUBLE
time              TIMESTAMP
_source           STRING
_source_ingest_ts TIMESTAMP
_raw_payload      STRING  -- optional
```

### Unified Table

```text
trades_unified
-------------------------
exchange      STRING
symbol        STRING
trade_id      STRING
side          STRING
price         DOUBLE
quantity      DOUBLE
trade_ts      TIMESTAMP
ingest_ts     TIMESTAMP
```

Partitioning (v1 suggestion):

- Partition `trades_unified` by `date(trade_ts)`
- Optionally add `symbol_bucket = bucket(16, symbol)` later.

---

## Testing

### Running Unit Tests

SchemaHub includes comprehensive unit tests for all core modules:

```bash
# Run all unit tests
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_checkpoint.py -v

# Run specific test class
python3 -m pytest tests/test_checkpoint.py::TestCheckpointManagerLocal -v

# Run with coverage report
python3 -m pytest tests/ --cov=schemahub --cov-report=html
```

### Unit Test Modules

- **test_checkpoint.py**: CheckpointManager (local/S3 storage, atomic writes, error handling)
- **test_seeds.py**: Seed file management (load/save product IDs, YAML parsing)
- **test_coinbase_connector_unit.py**: CoinbaseTrade, CoinbaseConnector, API pagination, data parsing
- **test_raw_writer_unit.py**: JSON Lines writing to S3, datetime serialization, error handling

### Test Coverage

Current test suite covers:
- Checkpoint load/save operations (local and S3)
- Seed file YAML parsing and writing with atomic operations
- Trade data deserialization and schema validation
- API pagination with cursor-based parameters
- Raw record transformation and serialization
- Error scenarios (missing files, corrupted data, S3 failures)

### Prerequisites for Testing

Tests use mocking to avoid external dependencies:

```bash
# Install dev dependencies
pip install -r requirements.txt

# No AWS credentials needed—tests stub S3 calls via botocore.Stubber
# No API keys needed—tests use mock HTTP responses
```

---

## Recommended Workflows (Continued)

5. **Resume interrupted backfills:**
   If a backfill crashes, simply re-run the same command. It will load checkpoints and continue from the last processed trade ID (no duplicates).

6. **Run `ingest` on a schedule for continuous microbatches:**
   Once backfill is complete, schedule the `ingest` command to run periodically (e.g., every 30 minutes) to capture recent trades.
   
   **For local/cron execution:**
   ```bash
   # Run every 30 minutes via cron
   0,30 * * * * cd /home/user/crypto_unification && python3 -m schemahub.cli ingest --s3-bucket my-bucket
   ```
   
   **For AWS Glue (recommended):**
   ```bash
   # Glue job script (ephemeral containers automatically track via watermark)
   python3 -m schemahub.cli ingest --s3-bucket my-bucket
   ```
   
   Benefits:
   - **Watermark-based**: automatically tracks and resumes from last fetched trade_id.
   - **No duplicates**: watermark prevents re-fetching across runs.
   - **Stateless**: works with ephemeral AWS Glue containers.
   - **Resumable**: if job fails, next run continues from watermark.
   - **Low overhead**: fetches only new trades since last run.

---

## Adding a New Product / End-to-End Pipeline Testing

When adding a new product or validating the full pipeline, follow this 6-step sequence. This tests the complete data flow from API → S3 raw → S3 curated → data quality checks.

**Testing New Features**: To validate new pipeline features without impacting production data, use low-volume coins like ACX-USD or ACS-USD for testing. These coins have minimal historical data (~1-5M trades) and allow quick validation of functionality changes.

### Full Pipeline Test Script

```bash
# 1. Full Backfill - Reset and fetch all historical trades
python -m schemahub.cli ingest PRODUCT-ID --s3-bucket $BUCKET --checkpoint-s3 --full-backfill

# 2. Transform #1 - Convert raw JSONL to unified Parquet
python -m schemahub.cli transform --s3-bucket $BUCKET

# 3. Incremental Ingest - Fetch only new trades since backfill
python -m schemahub.cli ingest PRODUCT-ID --s3-bucket $BUCKET --checkpoint-s3

# 4. Transform #2 - Process only the new raw file (incremental)
python -m schemahub.cli transform --s3-bucket $BUCKET

# 5. Transform --rebuild - Run Athena dedupe across all data
python -m schemahub.cli transform --s3-bucket $BUCKET --rebuild

# 6. Invoke Data Quality Lambda
aws lambda invoke --function-name schemahub-data-quality --payload '{}' /tmp/out.json
cat /tmp/out.json | jq -r '.body' | jq '{health_score, total_records, duplicates: .details.duplicates}'
```

### Expected Results (Example: BREV-USD)

| Step | Status | Before | After | Records | Notes |
|------|--------|--------|-------|---------|-------|
| **1. Full Backfill** | ✅ | cursor: 0 (reset) | cursor: 114505 | 114,999 raw | Wrote 2 files: 100,999 + 14,000 trades |
| **2. Transform #1** | ✅ | manifest: 0 files | manifest: 2 files | 114,999 parquet | Deduped within batches (100K + 14,999) |
| **3. Incremental Ingest** | ✅ | cursor: 114505 | cursor: 114527 | 2,000 raw | Picked up from trade 114505 → 114527 |
| **4. Transform #2** | ✅ | manifest: 2 files | manifest: 3 files | 2,000 parquet | Only processed 1 new raw file |
| **5. Transform --rebuild** | ✅ | (ignored manifest) | manifest: 3 files | 114,527 unique | Athena dedupe removed 347,589 dupes |
| **6. Lambda** | ✅ | - | - | - | Health: 50, Duplicates: 0 |

### Key Invariants to Verify

1. **Step 1 → Step 2**: Raw trade count ≈ Parquet record count (minor dedup within batches)
2. **Step 3 → Step 4**: Incremental raw delta matches incremental Parquet delta
3. **Step 5**: Final curated count = COUNT(DISTINCT exchange, symbol, trade_id)
4. **Step 6**: Lambda reports 0 duplicates in curated table

### Running on ECS Fargate

Replace local Python commands with ECS task runs:

```bash
# Ingest on ECS
aws ecs run-task \
  --cluster schemahub \
  --task-definition schemahub-ingest \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[SUBNET_ID],securityGroups=[SG_ID],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"ingest","command":["ingest","PRODUCT-ID","--s3-bucket","BUCKET","--checkpoint-s3"]}]}'

# Transform on ECS
aws ecs run-task \
  --cluster schemahub \
  --task-definition schemahub-ingest \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[SUBNET_ID],securityGroups=[SG_ID],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"ingest","command":["transform","--s3-bucket","BUCKET"]}]}'
```

### Pipeline Invariant Unit Tests

The test suite includes automated invariant checks in `tests/test_pipeline_invariants.py`:

```bash
pytest tests/test_pipeline_invariants.py -v
```

These tests verify (without AWS):
- Ingest count == Transform count
- No duplicates after batch dedupe
- Checkpoint cursor advances monotonically
- Incremental delta matches between stages
- Athena dedupe preserves exact unique count

---

## MVP Scope

The weekend MVP should include:

- Connectors for Binance and Coinbase trades.
- Raw Iceberg tables in S3:
  - `raw_binance_trades`
  - `raw_coinbase_trades`
- Two YAML mapping configs.
- One unified table in S3: `trades_unified`.
- Simple CLI:
  - `schemahub run ingest <source...>`
  - `schemahub run unify trades_unified`
- Basic watermark tracking per source.

This is already solid, realistic practice for building a mini internal data platform on AWS.

---

## Possible Future Extensions & Post-POC Improvements

**🤖 AI/ML Features Have Highest ROI:** Adding predictive models (price movement forecasting, anomaly detection on trade patterns, automated trading signals) would be the highest-impact enhancement. The unified data lake + historical archive makes this feasible immediately post-MVP.

### Performance & Optimization (Priority 1 - Post-POC)

These improvements should be tackled after the MVP is working, especially for large-scale backfills:

**Transform Pipeline Optimization:**
- **Incremental transform instead of full refresh**: Currently, the transform reads ALL raw JSONL files and retransforms them on every run (full refresh). For better performance and cost, implement incremental processing:
  - Use the manifest to track which raw files have already been transformed
  - On each run, only read raw files newer than the last checkpoint
  - Append new unified trades to existing Parquet files (or create new versioned files)
  - This reduces compute time from minutes to seconds for normal hourly runs, while `--rebuild` flag still supports full retransform when needed (e.g., after schema changes)

**API & Network Performance:**
- **Async/concurrent API calls within a product**: Currently, one product uses a single thread fetching trades sequentially. Implement `asyncio` or thread pools to fetch multiple trade IDs in parallel (e.g., fetch BTC trades AND ETH trades at the same time per worker).
- **Connection pooling & keep-alive**: Ensure `requests.Session` is properly using HTTP keep-alive to reuse TCP connections.
- **Adaptive backoff**: Implement exponential backoff for rate-limited or slow API responses instead of fixed 5s timeout.
- **Batch API requests**: If Coinbase offers bulk endpoints, use them instead of per-product requests.

**Parallelized Ingestion Within a Single Product:**
- **Current state**: Ingestion fetches trades sequentially—one API request completes before the next begins. The bottleneck is network I/O wait time, not CPU.
- **Opportunity**: Use concurrency to overlap API requests. Since we already track average request latency in metrics, we can calculate expected speedup using **Little's Law / concurrency math**:
  - If average request takes 200ms and we run 5 concurrent requests, theoretical throughput = 5x (limited by API rate limits)
  - Expected improvement: `speedup = min(concurrency_level, rate_limit / avg_requests_per_sec)`
- **Implementation approach**:
  - Use `asyncio` with `aiohttp` or `concurrent.futures.ThreadPoolExecutor`
  - Partition the trade ID range into chunks, fetch chunks in parallel
  - Merge results while preserving monotonic order for checkpoint updates
  - Respect Coinbase rate limits (~10 req/sec) to avoid 429s
- **Measurement plan**:
  1. Baseline: Record current avg request time and total backfill duration
  2. Calculate expected improvement using concurrency formula
  3. Implement parallel fetching with configurable concurrency (e.g., `--parallel 5`)
  4. Measure actual improvement and compare to prediction
  5. Tune concurrency level based on real-world rate limit behavior
- **Expected outcome**: 3-5x speedup on backfills (from hours to minutes for large products like BTC-USD)

**I/O Optimization & Storage Layout Experiments:**
- **Parquet layout experiments** (storage + scan perf primary focus):
  - Create `layout_experiments/` harness to rewrite unified data into 4–6 layouts:
    - Row group sizes: 64MB, 128MB, 256MB, 512MB targets
    - Compression: Snappy vs ZSTD
    - Sort orders: `trade_ts` vs `(symbol, trade_ts)` vs `(symbol, exchange)`
  - Measure per layout: file size, Athena bytes scanned (for standard query suite), query latency
  - Use this as primary artifact for demonstrating storage + scan performance tuning
- **Profile S3 write bottleneck**: Current implementation caches 100K trades locally, then writes ~50MB to S3. This is likely the slowest part (1-2s per write). Consider:
  - Multipart uploads for large files
  - Parallel uploads to S3 (while still fetching new data)
  - Compression (GZIP) to reduce payload size
- **Raw data format choice**: Currently using JSONL. Evaluate **Parquet** for raw data lake:
  - **JSONL pros**: Human-readable, streaming-friendly, schema-less
  - **JSONL cons**: Large file size, slower parsing, less efficient for analytics
  - **Parquet pros**: 10x smaller files (compression), faster to read, native support in Athena/Spark
  - **Parquet cons**: Requires schema upfront, batch writes instead of streaming
  - **Recommendation**: Use Parquet for raw data lake; trades don't change, so schema is stable.

**Checkpoint & Resume:**
- **Distributed checkpointing**: Multi-writer safety when scaling to many workers.
- **Checkpoint validation**: Detect and recover from corrupted checkpoints.

**Small Files → Compaction Loop (table format realism):**
- **Intentional microbatch compaction test**: Create pain and measure the fix:
  - Produce many small Parquet files in `state/` (e.g., one file per product per hour) to simulate realistic ingestion
  - Implement bin-pack compaction policies: "merge to 256MB files" or "compact per day per symbol bucket"
  - Measure: query planning time + execution time before/after compaction
  - Track file count and impact on Iceberg manifest size
- **Compaction instrumentation**: Log compaction cost (CPU, I/O, S3 request overhead) to understand economics

**Throughput Engineering as Explicit Subsystem:**
- **Stage model instrumentation** (fetch → transform → upload → commit):
  - Bound in-memory buffers to implement backpressure (prevent unbounded queue growth)
  - Separate thread pools for API fetch vs S3 write operations
  - Batch sizing controls: records per file, bytes per file, flush interval
  - Adaptive retry/backoff for 429s and timeouts
- **Measure steady-state performance**:
  - Trades/sec throughput
  - p95 API latency per fetch batch
  - Write throughput (MB/s to S3)
  - S3 request rate (multipart parts/sec)
  - Time spent in Iceberg commit vs data write (break down metadata overhead)
- **Create `bench/` harnesses**: scan bench (fixed query suite against each layout) + ingest bench (N-minute ingest run with queue/throughput stats)


**Distributed Ingestion Scaling:**

**Horizontal Scaling with ECS Tasks (Scale Out):**

The current architecture uses **vertical scaling** (single ECS task with internal `--workers` threading). For better cost efficiency and throughput, horizontal scaling launches multiple independent ECS tasks:

- **Current (Scale Up):** 1 ECS task → ThreadPool → N products in parallel
- **Proposed (Scale Out):** N ECS tasks → each claims/processes products independently

**Why this works easily:**
- Products are **embarrassingly parallel** (no cross-product dependencies)
- **DynamoDB per-product locks already exist** — workers can self-coordinate by claiming unclaimed products
- **Checkpointing is per-product** — no shared state between workers
- **Idempotent writes** — duplicate work is harmless (same JSONL files get overwritten)

**Implementation approach:**
1. Launch N ECS tasks (via EventBridge or Step Functions fan-out)
2. Each task loads the product seed file
3. Each task attempts to acquire per-product locks via DynamoDB
4. Tasks that acquire a lock process that product; others skip and try the next
5. Tasks exit when no unclaimed products remain

**Cost benefits:**
- Fargate Spot availability is better for smaller tasks (less interruption risk)
- N small tasks for 6 minutes each ≈ same compute as 1 large task for 60 minutes
- Lower per-vCPU-hour cost at smaller task sizes
- Faster completion (wall-clock time) for backfills

**Transform job scaling:**
- Transform is also parallelizable since it's per-file/per-product (no cross-product joins)
- Same pattern: multiple tasks claim raw files via manifest/lock, process independently
- Simpler than Spark since there's no shuffle/reduce phase

**Comparison to Spark/Ray:**
- ECS horizontal scaling is **simpler** (no cluster management, no driver coordination)
- Spark/Ray provide **more features** (automatic shuffle, fault tolerance, data locality)
- For this use case (embarrassingly parallel, no aggregations), ECS is sufficient
- Spark becomes valuable when adding cross-product analytics or complex transforms

**Alternative: PySpark or Ray for complex pipelines:**
- For larger scale operations with complex transforms, consider **PySpark** (for Spark clusters) or **Ray** (for distributed Python execution)
- These provide: automatic shuffle/reduce, fault tolerance, data locality optimization
- Useful when adding cross-product analytics (e.g., correlation analysis, aggregations across symbols)

### Other Future Extensions

Not required for MVP, but nice stretch goals:

- **True Partition-Based Incremental Transform**
  - Partition unified Parquet by `trade_date` (derived from `trade_ts`)
  - On each transform run, only read/merge/rewrite affected date partitions
  - Enables true incremental by PK `(exchange, symbol, trade_id)` without full table scan
  - Current approach uses Athena CTAS dedupe which scans entire table; partitioning scopes this to affected dates only
  - Benefits: O(partition size) not O(table size), partition pruning for queries

- **Apache Iceberg / Delta Lake Migration**
  - Replace plain Parquet with Iceberg table format
  - Native MERGE/UPSERT by PK without manual partition management
  - Atomic commits, time travel, schema evolution
  - File-level statistics for smart scan pruning
  - Automatic compaction and garbage collection
  - Requires: PyIceberg or Spark, Glue Iceberg catalog integration

- **Parquet Partitioning by Symbol**
  - Partition unified Parquet output by `symbol=X/` folders for query pruning
  - 10-100x faster Athena queries on specific symbols, lower cost (scan less data)
  - Consider `symbol + date` double-partitioning for high-volume symbols

- **Level 2 (L2) Order Book Data via WebSockets**
  - Extend the platform to ingest **real-time L2 order book data** (bid/ask depth) via Coinbase WebSocket feeds.
  - **Why this is compelling**: L2 data is significantly larger and more complex than trade data—capturing order book snapshots and deltas at millisecond granularity would demonstrate the platform's ability to handle high-throughput, low-latency streaming workloads.
  - **Data volume**: A single day of L2 data across major trading pairs could easily exceed 100GB+ of raw data, orders of magnitude larger than trade data alone.
  - **Use cases**: Order flow analysis, market microstructure research, liquidity metrics, spread analysis, and ML features for price prediction.
  - **Implementation**: WebSocket client for Coinbase's `level2` channel, efficient delta processing, and optimized Parquet storage for time-series order book snapshots.
  - **Resource requirements**: Modest compute (a small EC2 instance or local machine) could handle ingestion for a single day demo—cost would be minimal (~$1-5 for a full day of data collection).

- **Scale to Many Products (early demand test)**
  - Currently testing with a handful of products (e.g., BTC-USD, ETH-USD). Scale to 50–100+ Coinbase products to test:
    - Checkpoint file count and manifest overhead (does resuming get slower?)
    - Thread pool contention and queueing behavior under load
    - S3 request rate limits (429s) and retry backoff effectiveness
    - Memory footprint with many concurrent fetch/write buffers
    - Query planning time in Athena as file counts grow
  - This cheap test reveals bottlenecks early (API rate limiting, S3 throughput, memory, Iceberg metadata scaling) before building out distributed infrastructure.

- **Orchestration & Workflow Management (Prefect, Dagster, or Airflow)**
  - **Why orchestration?** Currently, ingestion and backfill jobs are manually triggered or scheduled via AWS Glue/cron. An orchestration framework would provide:
    - **Unified pipeline management**: Define the entire flow (fetch product seed → ingest/backfill → unify → validate) as a DAG (directed acyclic graph) with clear dependencies.
    - **Centralized monitoring & alerting**: Track job status, failure rates, and duration across all data sources in a single dashboard. Alert on failures or SLA violations.
    - **Automated retry & recovery**: Built-in exponential backoff, multi-attempt handling, and task-level recovery without manual intervention.
    - **Data lineage & audit trails**: Track which trades came from which API calls, when they were ingested, and how they flowed through transformations.
- **Cost optimization**: Visualize resource usage per job (API calls, S3 writes, compute), optimize worker counts and batch sizes.
    - **Incremental backfill orchestration**: Automatically parallelize backfills across products, manage concurrency, and resume from checkpoints without manual CLI invocation.
    - **Multi-source coordination**: When adding new exchanges (Kraken, Binance, etc.), orchestrate their ingestion, unification, and data quality checks as a single cohesive workflow.
    - **Scheduling flexibility**: Beyond simple cron, support event-driven triggers (e.g., "ingest when product list changes") and dynamic scheduling based on data volumes.
  - **Tool recommendations**:
    - **Prefect** (lightweight, Python-native, modern UI): Best for rapid development and dynamic workflows.
    - **Dagster** (comprehensive, strong data assets, excellent observability): Best for long-term maintainability and complex multi-source pipelines.
    - **Apache Airflow** (battle-tested, ecosystem-rich): Best if scaling to enterprise complexity with large teams.

- Automatic schema evolution
  - Detect new fields in raw tables.
  - Propose or auto-generate updates to mapping configs + unified schema.
- Advanced incremental ingestion
  - Store both `last_trade_id` and `last_ts` per source.
  - Handle late-arriving data.
- Atomic multi-source commits
  - Ingest from multiple exchanges, unify, and publish as a single Iceberg snapshot.
- Partition planning
  - Automatically suggest partition specs for `trades_unified` based on data volume and query patterns.

- **Operational Metrics Dashboard**
  - After the pipeline runs for 1-2 weeks, create visualizations to demonstrate system efficiency and reliability:
    - Data volume growth trends (cumulative records over time)
    - Storage efficiency (raw JSONL vs Parquet size, compression ratio)
    - Pipeline success rate and failure patterns
    - Records per source/product distribution
    - Processing speed trends (records/second)
    - Data freshness metrics (latest trade timestamp)
    - Scan performance metrics: bytes scanned per query, latency trends across layouts
    - Throughput metrics: trades/sec, p95 API latency, MB/s write rate over time
  - Use Athena to query operational data and build simple charts for portfolio/resume demonstration.
- More domains
  - Reuse the same pattern for e-commerce events, ad impressions, etc. Only the connectors, mappings, and target schema change.
- Infrastructure as Code (Terraform)
  - Create Terraform modules to automate provisioning of AWS resources (S3 buckets, IAM roles, Glue jobs, Iceberg catalogs).
  - Enable reproducible, version-controlled infrastructure deployments and easy multi-environment setups (dev, staging, prod).

### BYO-Keys Exchange Gateway (Hosted Product Direction)

**Vision:** Transform SchemaHub from a personal data platform into a **hosted multi-tenant exchange gateway** where users bring their own API keys and get a unified, reliable interface to all major centralized exchanges.

**Why this matters:** The current architecture fetches public trade data. A BYO-keys gateway extends this to **authenticated operations**—placing orders, checking balances, managing positions—across exchanges through a single API. This is the nucleus of a hosted product.

**Theoretical Background:**

Exchange gateways sit at the intersection of several important distributed systems concepts:

- **API Gateway Pattern**: A single entry point that routes requests to multiple backend services (exchanges), handling cross-cutting concerns like auth, rate limiting, and monitoring. Similar to Kong, Envoy, or AWS API Gateway but specialized for trading.
- **Circuit Breaker Pattern**: Prevents cascading failures when an exchange goes down. Instead of hammering a failing exchange (and potentially getting rate-limited or banned), the circuit "opens" after N failures and fails fast for a cooldown period. Classic reliability pattern from Netflix's Hystrix.
- **Health Check Patterns**: Canary endpoints that continuously probe exchange connectivity and latency. Distinguishes between "exchange is slow" vs "exchange is down" vs "our credentials expired."
- **Idempotency in Distributed Systems**: When placing orders, network failures create ambiguity—did the order go through? Idempotency keys let you safely retry without double-ordering. Critical for financial operations.
- **Multi-tenant Resource Isolation**: Each user's API keys, rate limits, and quotas are isolated. One user hitting rate limits shouldn't affect others.

**Reference Implementation:** [ccxt-rest](https://github.com/ccxt-rest/ccxt-rest) provides the "single REST API" shape—it wraps [CCXT](https://github.com/ccxt/ccxt) (the standard library for exchange connectivity) in a REST service. This is the starting point, not the end state.

**Architecture Layers:**

```
┌─────────────────────────────────────────────────────────────┐
│                    User Applications                        │
│              (Trading bots, dashboards, etc.)               │
└─────────────────────────┬───────────────────────────────────┘
                          │ Unified REST API
┌─────────────────────────▼───────────────────────────────────┐
│                   Exchange Gateway Service                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Auth/Keys   │  │ Rate Limiter│  │ Metering & Quotas   │  │
│  │ Management  │  │ (per-user)  │  │ (usage tracking)    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Circuit Breaker Manager                    ││
│  │  (per exchange + method, with health state machine)     ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Idempotency Key Store                      ││
│  │  (DynamoDB: request deduplication for order placement)  ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    CCXT Adapter Layer                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Coinbase │  │ Binance  │  │ Kraken   │  │ ...more  │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
└─────────────────────────────────────────────────────────────┘
```

**Implementation Roadmap:**

**Phase 1: CCXT Service Foundation**
- Deploy ccxt-rest or build minimal FastAPI wrapper around CCXT
- Support authenticated endpoints: `fetchBalance`, `createOrder`, `fetchOpenOrders`, `cancelOrder`
- Secure API key storage (AWS Secrets Manager or encrypted DynamoDB)
- Basic request logging and audit trail

**Phase 2: Reliability Layer**
- **Canary health checks**: Background tasks that periodically call lightweight endpoints (`fetchTime`, `fetchTicker`) to detect exchange issues before users hit them
- **Circuit breaker per exchange+method**: Track failure rates. States: CLOSED (normal) → OPEN (failing fast) → HALF-OPEN (testing recovery). Configurable thresholds (e.g., open after 5 failures in 60s, try recovery after 30s)
- **Graceful degradation**: When Binance is down, route to backup exchange or return cached data with staleness indicator

**Phase 3: Order Safety & Idempotency**
- **Idempotency keys for order placement**: Client provides a unique key per order intent. Gateway stores key→order_id mapping in DynamoDB. Retries with same key return existing order instead of creating duplicate
- **Order state reconciliation**: Background job that syncs local order cache with exchange state (handles orders placed directly on exchange)
- **Webhook/callback support**: Notify users of order fills, cancellations, etc.

**Phase 4: Multi-Tenant Metering**
- **Per-user rate limiting**: Prevent any single user from exhausting shared exchange rate limits
- **Usage metering**: Track API calls, orders placed, data volume per user
- **Quota enforcement**: Free tier limits, paid tier upgrades
- **Billing integration**: Usage-based pricing (Stripe metered billing)

**Why CCXT as Foundation:**
- Supports 100+ exchanges out of the box
- Handles exchange-specific quirks (auth schemes, parameter formats, error codes)
- Active maintenance and community
- Adding a new exchange = configuration, not code

**Scaling Considerations:**
- Stateless gateway instances behind ALB (horizontal scaling)
- Redis for shared circuit breaker state and rate limit counters
- DynamoDB for idempotency keys and user quotas
- Separate worker fleet for health checks (don't block user requests)

**Security Considerations:**
- API keys encrypted at rest (KMS)
- Keys never logged or exposed in errors
- Per-user isolation (user A can't use user B's keys)
- Audit logging for compliance

This direction transforms SchemaHub from a data ingestion tool into the **control plane for multi-exchange trading**, with the data lake providing the historical context and analytics layer.

---

## Learning Path: Data Lake Performance & Storage Internals

Expand into actually interesting areas now that you have the main stuff set up. Play around with storage, Iceberg, checkpointing, API performance, real-time streaming, and beyond.

**Short answer:** you'll get the most "this is sick" energy by reading about:

- How columnar formats (Parquet) and table formats (Iceberg/Hudi/Delta) actually work.
- How engines (Spark/Glue, Athena, warehouses) physically execute queries.
- Classic data-lake performance problems (small files, partitioning, caching) and how big shops solved them.

Below is a curated "syllabus" with concrete article types and some specific examples.

### 1. Columnar Storage & Parquet Internals

**Why:** This is the foundation for storage + query perf. Once you grok Parquet, partitioning/file-size discussions become intuitive.

**Topics to look for:**

- How Parquet stores data (row groups, column chunks, pages).
- Encodings: dictionary, RLE, bit-packing, etc.
- How Parquet statistics (min/max, null counts) allow skipping whole chunks.
- Tradeoffs: wide vs narrow tables, many small vs fewer wide columns.

**Example resources:**

- Official Parquet encoding spec (low-level but eye-opening).
- Explainer-style posts on Parquet encodings and optimization.
- Articles on dictionary encoding and when it helps.

**Focus your reading on:** "How does this help engines read less data and scan fewer bytes?"

### 2. The "Small Files Problem" and File-Size Tuning

**Why:** Your S3 + Glue + Athena stack will absolutely hit this, and it's one of the most satisfying problems to solve.

**Topics:**

- Why millions of small objects on S3 crush performance.
- Optimal file sizes (128–512 MB range) and how Spark configs (`maxPartitionBytes`, etc.) play in.
- Compaction strategies: daily compaction jobs, auto-optimize features in table formats.
- How table formats provide built-in optimizations (Iceberg compaction, Delta "optimize write", etc.).

**Example resources:**

- General "small file problem" explanations in data lakes.
- Spark-oriented small-file discussions and recommendations on ideal file sizes.
- How Iceberg/Delta/Hudi handle small-file optimization in a data lake.

**When you read, mentally map:** "How would I implement a compaction job in Glue to fix this?"

### 3. Table Formats: Iceberg vs Hudi vs Delta Lake

**Why:** This is the "lakehouse" core – upserts, schema evolution, and smart partitioning. Even if you don't adopt one yet, understanding them will sharpen how you design your curated layer.

**Topics:**

- What a "table format" is (metadata & manifests on top of Parquet files).
- How they handle:
  - ACID transactions on S3.
  - Partitioning (hidden partitions, partition evolution).
  - Time travel and incremental reads.
  - Performance features: metadata pruning, manifest lists, clustering.

**Example resources:**

- Deep-dive comparison blog posts (capabilities, performance, use cases).
- Posts focused specifically on partitioning in these formats.

**Read with the question:** "If I had to migrate my Coinbase curated tables to Iceberg a year from now, which features would I lean on?"

### 4. Spark/Glue Performance Tuning & Query Execution

**Why:** This is where you directly affect runtime + cost for your Glue jobs.

**Topics:**

- How Spark's Catalyst optimizer works at a high level.
- Partitioning and shuffle strategies.
- Join strategies: broadcast vs shuffle hash vs sort-merge.
- Configs that actually matter: `shuffle.partitions`, broadcast thresholds, caching.
- Reading Spark UI / Glue job metrics to debug bottlenecks.

**Example resources:**

- Official Spark SQL performance tuning documentation.
- AWS prescriptive guidance specifically for tuning Glue for Spark.
- Practical Spark tuning "best practices" articles.

**As you read, keep translating back to your project:** "For a 30-minute Coinbase micro-batch, what setting would I tweak and why?"

### 5. Athena & Query-Engine Optimization

**Why:** Athena will probably be your first query surface on S3, and it's very sensitive to storage design.

**Topics:**

- How Athena charges (data scanned) and why columnar + partitioning matter.
- Partition keys vs partition projection.
- Bucketing and how it interacts with joins.
- Writing queries that minimize scanned data (pruning + column selection).

**Example resources:**

- AWS "Top 10 performance tuning tips for Amazon Athena" (classic but still relevant).
- More recent best-practices papers/blog posts on Athena query optimization.

**When you read these, think:** "Given my S3 layout, what partitions and file sizes do they implicitly recommend?"

### 6. Big-Picture Data-Lake/Lakehouse Performance Design

Once you're comfortable with the building blocks, go a bit more "architecture nerd":

**Topics:**

- Lakehouse patterns on S3: raw vs curated vs serving layers.
- How companies like Netflix/Uber/Airbnb structure their data lakes for performance.
- Cost-based optimization and how metadata (stats, histograms) is used.
- Caching layers (e.g., Alluxio, in-memory caching in Spark, warehouse result caches).

**You can search for:**

- "Netflix Iceberg performance"
- "Uber Hadoop data lake optimization"
- "data lakehouse performance architecture blog"

and skim the war stories.

### 7. When the Lakehouse Actually Wins: Cost Comparison

**Why:** Knowing when to use a table format vs just storing Parquet on S3 is a critical business decision.

**Topics:**

- Iceberg vs Snowflake vs plain Parquet on S3: total cost of ownership.
- Query performance vs infrastructure complexity tradeoffs.
- When table format overhead is worth it vs when raw Parquet wins.

**Resource:**

- [Iceberg vs Snowflake cost comparison vs Parquet on S3](https://chatgpt.com/c/690e6747-ed54-832b-ad11-107f88235eb2)

### 8. How to Actually Study This (Without Getting Lost)

To make this exciting instead of overwhelming, I'd do:

**Week 1: Storage & Parquet**

1–2 evenings:

- Read a Parquet intro + encoding explainer.
- Take one of your small Coinbase tables, write it as Parquet with different compressions/encodings, and compare file size + query time.

**Week 2: Small Files + Partitions**

- Read 1–2 "small files problem" posts + an Iceberg optimization article.
- Add a simple compaction step in your Glue pipeline and measure Athena query speed before vs after.

**Week 3: Spark/Glue Internals**

- Read Spark and Glue tuning guides.
- Turn on the Glue/Spark UI, run your job, and try to interpret one bad stage and make it faster.

**Week 4: Athena Optimization & Table Formats**

- Read Athena tuning blogs.
- Read a modern comparison of Iceberg vs Hudi vs Delta.
- Sketch how your curated `fact_trades` would look if you moved it to Iceberg.

---

## Repository Layout

```text
schemahub/
  README.md
  schemahub/
    __init__.py
    cli.py              # schemahub run ...
    connectors/
      binance.py
      coinbase.py
      kraken.py
    raw_loader.py       # write to raw_* tables
    unifier.py          # apply mappings → trades_unified
    transforms.py       # from_ms, iso8601, to_double, etc.
    meta/
      watermarks.py     # track per-source high watermarks
  config/
    mappings/
      binance_trades.yaml
      coinbase_trades.yaml
      kraken_trades.yaml
  tests/
    ...
```

Happy hacking — this gives you a realistic, end-to-end crypto trades platform on Iceberg + S3 that you fully own and understand.

---

## Backfill Progress Tracking

### Overview

This feature adds real-time progress tracking to the backfill (`--full-refresh`) process. When running a full refresh, the system now:

1. **Scans all products upfront** to determine the latest trade_id for each product
2. **Calculates total records** to be processed across all products
3. **Displays real-time progress updates** after each batch write to S3 showing:
   - Progress bar visualization
   - Percentage complete
   - Records processed / Total records
   - Processing rate (records per minute)
   - Estimated time remaining
   - Per-product breakdown (top 10 slowest products)
4. **Shows a final summary** when the backfill completes

### Usage

#### Single Product Backfill

```bash
# Full refresh for a single product with progress tracking
python -m schemahub.cli ingest BTC-USD \
  --full-refresh \
  --s3-bucket schemahub-crypto-533595510108 \
  --checkpoint-s3
```

**Output Example:**
```
Scanning 1 products to calculate total records to process...
Scan complete in 0.5 seconds. Starting backfill...

  BTC-USD: wrote 100000 trades (cursor=200500, target=15234567)

================================================================================
BACKFILL PROGRESS UPDATE - 2026-01-20 12:15:30 UTC
================================================================================
[██████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] 20.5%
Records: 3,124,567 / 15,234,567
Rate: 52,076 records/minute
Elapsed: 60.0 minutes
ETA: 233.0 minutes (3.9 hours)
Products: 1 total

Per-Product Progress (showing up to 10):
  BTC-USD         [████░░░░░░░░░░░░░░░░] 20.5% (3,124,567 / 15,234,567)
================================================================================
```

#### Multi-Product Backfill

```bash
# Full refresh for all products in seed file with parallel workers
python -m schemahub.cli ingest \
  --full-refresh \
  --s3-bucket schemahub-crypto-533595510108 \
  --checkpoint-s3 \
  --workers 4
```

Note: When running without a specific product, it loads all products from `config/mappings/product_ids_seed.yaml`

**Output Example:**
```
Scanning 765 products to calculate total records to process...
  Scanned 10/765 products...
  Scanned 20/765 products...
  ...
Scan complete in 127.3 seconds. Starting backfill...

================================================================================
BACKFILL PROGRESS UPDATE - 2026-01-20 14:30:45 UTC
================================================================================
[████████████████████████████░░░░░░░░░░░░░░░░░░░░░░] 56.3%
Records: 234,567,890 / 416,789,234
Rate: 1,952,283 records/minute
Elapsed: 120.0 minutes
ETA: 93.2 minutes (1.6 hours)
Products: 765 total

Per-Product Progress (showing up to 10):
  AAVE-USD        [███░░░░░░░░░░░░░░░░░]  15.2% (45,678 / 300,123)
  1INCH-USD       [████░░░░░░░░░░░░░░░░]  20.1% (12,345 / 61,432)
  BTC-USD         [██████████████░░░░░░]  70.5% (10,765,432 / 15,234,567)
  ETH-USD         [████████████████░░░░]  80.2% (8,234,567 / 10,267,890)
  ...
================================================================================
```

#### Incremental Ingest (No Progress Bar)

The progress bar is **only** enabled for `--full-refresh` backfills. Regular incremental ingests run without the progress tracker:

```bash
# Incremental ingest (no progress bar)
python -m schemahub.cli ingest BTC-USD \
  --s3-bucket schemahub-crypto-533595510108 \
  --checkpoint-s3
```

### ECS Usage

When running via ECS tasks, the progress output will appear in the CloudWatch logs for the task:

```bash
# Full refresh with progress tracking in ECS
aws ecs run-task \
  --cluster schemahub \
  --task-definition schemahub-ingest \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-010fe3e4e9994464b],securityGroups=[sg-06dfef37f4737c6e9],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"ingest","command":["ingest","AAVE-USD","--s3-bucket","schemahub-crypto-533595510108","--checkpoint-s3","--full-refresh"]}]}' \
  --query 'tasks[0].taskArn' --output text
```

You can then view the progress updates in CloudWatch Logs for the task.

### Configuration

#### Progress Bar Width

The progress bar width is 50 characters by default. This can be changed in [schemahub/progress.py:135](schemahub/progress.py#L135):

```python
bar_width = 50  # Change to desired width
```

### Implementation Details

#### Architecture

The feature consists of two main components:

1. **ProgressTracker Class** ([schemahub/progress.py](schemahub/progress.py))
   - Thread-safe progress tracking across multiple products
   - Calculates rates, ETAs, and progress percentages
   - Generates formatted progress bar output

2. **CLI Integration** ([schemahub/cli.py](schemahub/cli.py))
   - Scans all products upfront to get target trade IDs (lines 313-333)
   - Passes ProgressTracker to `ingest_coinbase()` function
   - Updates progress after each batch write (lines 150-153, 184-186)
   - Prints updates immediately after each batch write to S3
   - Shows final summary at completion (line 427)

#### How It Works

1. **Initial Scan**: When `--full-refresh` is specified, the CLI scans all products to fetch their latest trade_id using the Coinbase API
2. **Registration**: Each product is registered with the ProgressTracker along with its start cursor (1000) and target trade_id
3. **Progress Updates**: As the `ingest_coinbase()` function writes batches to S3, it calls `progress_tracker.update_progress()` with the number of records processed
4. **Immediate Display**: After each batch write to S3, the tracker prints a progress update immediately
5. **Final Summary**: When all products are complete, a final summary is printed with total statistics

#### Thread Safety

The ProgressTracker uses a threading.Lock to ensure thread-safe updates when running with `--workers > 1`. This prevents race conditions when multiple product ingestion threads update progress simultaneously.

### Benefits

- **Visibility**: See real-time progress during long-running backfills
- **ETA Calculation**: Know approximately how long the backfill will take
- **Rate Monitoring**: Monitor processing throughput (records/minute)
- **Debugging**: Identify slow products or processing bottlenecks
- **Confidence**: Confirm the backfill is making progress (not stuck)

### Files Modified

- [schemahub/cli.py](schemahub/cli.py#L153) - Force progress updates on every batch write
- [schemahub/progress.py](schemahub/progress.py) - ProgressTracker class (unchanged, supports force parameter)

---

## Exchange API Health Tracking & Circuit Breaker

### Overview

SchemaHub includes an AWS-native health tracking system that monitors exchange API health and implements circuit breaker patterns to prevent cascading failures during ingestion. This is especially important for multithreaded ingestion where multiple workers could hammer a failing API.

### Key Features

- **Real-time Health Tracking**: Monitors API response times, error rates, and consecutive failures
- **Circuit Breaker Pattern**: Automatically pauses requests when exchanges become unhealthy
- **DynamoDB-Backed State**: Shared health state across all workers (thread-safe)
- **CloudWatch Metrics**: Publishes health metrics for dashboards and alarms
- **Smart Retry Logic**: Coordinated waiting when circuit opens (prevents thundering herd)
- **Automatic Recovery**: Tests recovery after cooldown period

### How It Works

#### Circuit States

The circuit breaker has three states:

1. **CLOSED** (Healthy): Normal operation, all requests proceed
2. **OPEN** (Unhealthy): API is failing, all threads wait 5 minutes before retrying
3. **HALF_OPEN** (Testing): ONE thread tests recovery, others wait

#### State Transitions

```
CLOSED → OPEN: After 5 consecutive failures
OPEN → HALF_OPEN: After 5-minute cooldown
HALF_OPEN → CLOSED: After 3 consecutive successes
HALF_OPEN → OPEN: If recovery test fails
```

#### Behavior During Failures

When the Coinbase API returns 500 errors:

1. **Failure 1-4**: Normal retry with exponential backoff (30s, 60s, 120s)
2. **Failure 5**: Circuit OPENS - all threads pause
3. **Wait Period**: All threads wait 5 minutes (shared via DynamoDB)
4. **Recovery Test**: ONE thread tests the API (atomic DynamoDB operation)
5. **Success**: Circuit closes, all threads resume
6. **Failure**: Circuit stays open, wait another 5 minutes

### Configuration

Health tracking is configured via environment variables in your `.env` file:

```bash
# Health Check Configuration
DYNAMODB_HEALTH_TABLE=schemahub-exchange-health
CIRCUIT_BREAKER_ENABLED=true
HEALTH_CHECK_ENABLED=true
AWS_REGION=us-east-1
```

### AWS Infrastructure

#### DynamoDB Table

The `schemahub-exchange-health` table stores health state:

- **Partition Key**: `exchange_name` (e.g., "coinbase")
- **Sort Key**: `timestamp` (ISO8601)
- **TTL**: Auto-deletes records after 7 days
- **Attributes**: 
  - `status`: "healthy" | "degraded" | "unhealthy"
  - `circuit_state`: "closed" | "open" | "half_open"
  - `consecutive_failures`: Integer
  - `consecutive_successes`: Integer
  - `avg_response_time_ms`: Decimal (moving average)
  - `error_rate`: Decimal (rolling window of last 100 requests)
  - `last_success_ts`: ISO8601 timestamp
  - `last_failure_ts`: ISO8601 timestamp
  - `last_error_message`: String (truncated to 500 chars)

**View health state:**
```bash
aws dynamodb scan --table-name schemahub-exchange-health --max-items 5
```

**View failures only:**
```bash
aws dynamodb scan --table-name schemahub-exchange-health \
  --filter-expression "consecutive_failures > :zero" \
  --expression-attribute-values '{":zero": {"N": "0"}}' \
  --max-items 5
```

#### CloudWatch Metrics

Published to the `SchemaHub` namespace:

- `ExchangeHealthy`: 1 (healthy) or 0 (unhealthy)
- `ExchangeResponseTime`: API response time in milliseconds
- `ExchangeErrorRate`: Error rate (0.0 to 1.0)
- `CircuitBreakerState`: 0 (closed), 0.5 (half_open), 1 (open)

**Query metrics:**
```bash
aws cloudwatch get-metric-statistics \
  --namespace SchemaHub \
  --metric-name ExchangeHealthy \
  --dimensions Name=Source,Value=coinbase \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average
```

### Multithreaded Behavior

When running with `--workers > 1`:

1. **Shared State**: All threads query the same DynamoDB health table
2. **Coordinated Waiting**: When circuit opens, all threads wait the same 5 minutes
3. **Atomic Recovery**: Only ONE thread tests recovery (DynamoDB conditional write)
4. **Immediate Propagation**: When circuit closes, all threads resume immediately

**Example with 4 workers:**
```
Thread 1 (BTC-USD):  500 error → consecutive_failures = 1
Thread 2 (ETH-USD):  500 error → consecutive_failures = 2
Thread 3 (SOL-USD):  500 error → consecutive_failures = 3
Thread 1 (BTC-USD):  500 error → consecutive_failures = 4
Thread 4 (DOGE-USD): 500 error → consecutive_failures = 5 → CIRCUIT OPENS

[All threads wait 5 minutes]

Thread 2 (ETH-USD):  Tests recovery → SUCCESS → circuit CLOSED
Thread 1 (BTC-USD):  Resumes normal operation
Thread 3 (SOL-USD):  Resumes normal operation
Thread 4 (DOGE-USD): Resumes normal operation
```

### Implementation Details

#### Core Components

1. **Health Tracker** ([schemahub/health.py:92](schemahub/health.py#L92))
   - `ExchangeHealthTracker` class manages DynamoDB operations
   - Thread-safe using DynamoDB's atomic operations
   - Lazy-loads DynamoDB client and table

2. **Circuit Breaker** ([schemahub/health.py:274](schemahub/health.py#L274))
   - `CircuitBreaker` class implements state machine
   - `get_wait_time()`: Returns wait time based on circuit state
   - `should_test_recovery()`: Atomic check for recovery testing
   - `record_success()` / `record_failure()`: Update health state

3. **Connector Integration** ([schemahub/connectors/coinbase.py:127](schemahub/connectors/coinbase.py#L127))
   - Circuit breaker checks BEFORE each retry attempt
   - Records success/failure AFTER each API call
   - Publishes CloudWatch metrics on every request

#### Retry Logic Changes

**Before (without circuit breaker):**
```python
for attempt in range(1, max_retries + 1):
    try:
        response = session.get(url)
        response.raise_for_status()
        break
    except HTTPError:
        if attempt < max_retries:
            time.sleep(backoff_time)
            continue
        raise
```

**After (with circuit breaker):**
```python
for attempt in range(1, max_retries + 1):
    # Check circuit state
    wait_time = circuit_breaker.get_wait_time("coinbase", attempt)
    if wait_time > 0:
        logger.warning(f"Circuit-aware wait: {wait_time}s")
        time.sleep(wait_time)
    
    try:
        response = session.get(url)
        circuit_breaker.record_success("coinbase", elapsed_ms)
        response.raise_for_status()
        break
    except HTTPError:
        circuit_breaker.record_failure("coinbase", error_msg)
        if attempt < max_retries:
            continue
        raise
```

### Benefits

1. **Prevents Cascading Failures**: Stops all threads from hammering failing APIs
2. **Reduces API Costs**: Avoids wasted requests during outages
3. **Better Error Messages**: Logs clearly show when circuit opens/closes
4. **Automatic Recovery**: No manual intervention needed
5. **Production Ready**: Battle-tested circuit breaker pattern
6. **Observable**: Full visibility via DynamoDB and CloudWatch

### Testing

To test the circuit breaker locally:

```bash
# Run ingestion with health checks enabled
source .venv/bin/activate
python -m schemahub.cli ingest 1INCH-USD --limit 100 --workers 1

# Monitor health state in DynamoDB
aws dynamodb scan --table-name schemahub-exchange-health --max-items 10

# Check logs for circuit events
tail -f /tmp/ingest_test.log | grep -i "circuit\|health"
```

**Expected log output during circuit opening:**
```
ERROR - [API] 1INCH-USD: *** SERVER ERROR 5XX DETECTED *** status=500
ERROR - [API] 1INCH-USD: Will retry (attempt 2/3)
ERROR - [API] 1INCH-USD: *** SERVER ERROR 5XX DETECTED *** status=500
ERROR - coinbase circuit OPENED after 5 consecutive failures. Last error: HTTP 500
WARNING - [API] 1INCH-USD: Circuit-aware wait: 300s before attempt 3/3
INFO - coinbase circuit → HALF_OPEN (testing recovery)
INFO - coinbase circuit CLOSED - exchange recovered! (3 consecutive successes)
```

### Files Modified

- [schemahub/health.py](schemahub/health.py) - New file with health tracking and circuit breaker
- [schemahub/connectors/coinbase.py](schemahub/connectors/coinbase.py#L19) - Import circuit breaker
- [schemahub/connectors/coinbase.py](schemahub/connectors/coinbase.py#L127) - Integrate health checks
- [schemahub/metrics.py](schemahub/metrics.py#L160) - Add new health metric methods
- [terraform/dynamodb.tf](terraform/dynamodb.tf#L38) - Create exchange_health table
- [terraform/iam.tf](terraform/iam.tf#L152) - Add DynamoDB permissions for health table
- [.env.example](.env.example#L16) - Document health check configuration

### Disabling Health Checks

To disable health tracking (e.g., for local development without AWS):

```bash
# In your .env file
CIRCUIT_BREAKER_ENABLED=false
HEALTH_CHECK_ENABLED=false
```

When disabled, the system behaves exactly as before - standard retry logic with exponential backoff.



## Ops Dashboard Review Guide

**Dashboard:** `schemahub-data-quality` in CloudWatch (us-east-1)

### Quick Health Check

| Metric | Red Flag |
|--------|----------|
| Health Score | < 50 Critical, < 80 Degraded |
| Total Records | Sudden drops = data loss |
| Daily Records | 0 = pipeline stalled |
| Duplicates | > 0 = run `transform --rebuild` |
| Stale Products | > 0 = check ingest jobs |

### Common Fixes

| Issue | Fix |
|-------|-----|
| Stale data | Check ECS tasks, EventBridge schedule |
| Duplicates | `python -m schemahub.cli transform --rebuild` |
| Pipeline stuck | Check DynamoDB `schemahub-locks` table |

### Manual Lambda Trigger

```bash
aws lambda invoke --function-name schemahub-data-quality --payload '{}' /tmp/out.json
cat /tmp/out.json | jq -r '.body' | jq '{health_score, total_records}'
```

### API Error Monitoring

**Dashboard:** `schemahub-dashboard` in CloudWatch (us-east-1) - Row 4 "Operational Error Metrics"

#### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `RateLimitErrors` | HTTP 429 errors (Coinbase rate limiting) | >10/min warning, >50/min critical |
| `ServerErrors` | HTTP 5xx errors from Coinbase | Any = investigate |
| `TimeoutErrors` | Request timeouts (>15s) | >5/min = API slowdown |
| `ConnectionErrors` | Network connectivity failures | Any = check VPC/NAT |
| `CircuitBreakerOpens` | Circuit breaker triggered | Any = exchange issue |
| `APISuccessCount` | Successful API calls | For calculating success rate |

#### Error Rate Formula

```
Error Rate = (RateLimitErrors + ServerErrors + TimeoutErrors + ConnectionErrors) / (APISuccessCount + all errors)
```

Target: < 3% error rate (0.03)

#### Circuit Breaker States

| State | Value | Meaning |
|-------|-------|---------|
| Closed | 0 | Normal operation |
| Half-Open | 0.5 | Testing recovery (1 request) |
| Open | 1 | API blocked for 5 min cooldown |

#### CloudWatch Insights Queries

```sql
-- Count errors by type (last 24h)
SOURCE '/ecs/schemahub'
| filter @message like /RATE LIMITED|SERVER ERROR|TIMEOUT|CONNECTION/
| stats count() as errors by bin(1h)

-- 429 Rate Limit errors with product
SOURCE '/ecs/schemahub'
| filter @message like /RATE LIMITED/
| parse @message /\[API\] (?<product>[A-Z]+-[A-Z]+):/
| stats count() as rate_limits by product
| sort rate_limits desc
| limit 10

-- Circuit breaker events
SOURCE '/ecs/schemahub'
| filter @message like /circuit OPENED|circuit CLOSED|recovery/
| sort @timestamp desc
| limit 20
```

#### Common Issues and Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| High 429s | Too many parallel requests | Reduce `--chunk-concurrency` |
| Circuit opens frequently | Exchange instability | Wait, check Coinbase status |
| Stale circuit blocking | Old OPEN state in DynamoDB | Auto-resets after 10 min |
| Timeouts increasing | Coinbase API slow | Normal - built-in retries handle it |

### Key Athena Queries

```sql
-- Health overview
SELECT COUNT(*) as total, MAX(trade_ts) as latest FROM curated_trades;

-- Duplicates
SELECT symbol, COUNT(*) - COUNT(DISTINCT trade_id) as dups
FROM curated_trades GROUP BY symbol HAVING COUNT(*) > COUNT(DISTINCT trade_id);
```

### Notes

- Lambda runs every **6 hours** - trigger manually for real-time check
- Checkpoints: `s3://bucket/schemahub/checkpoints/`
- Logs: `/aws/lambda/schemahub-data-quality`, `/ecs/schemahub`
