# Component Details

Detailed reference for CLI commands, parameters, environment variables, and internal components.

---

## CLI Commands

### `ingest` — Incremental Trade Ingestion

**Use case:** Fetch new trades incrementally using watermark checkpoints. Ideal for scheduled jobs (every 30-60 minutes).

```bash
python3 -m schemahub.cli ingest [PRODUCT] [OPTIONS]
```

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `product` | (optional) | Coinbase product ID (e.g., `BTC-USD`). If omitted, reads from seed file. |
| `--seed-path` | `config/mappings/product_ids_seed.yaml` | Path to product seed YAML. |
| `--limit` | 1000 | Trades per API request (max 1000). |
| `--s3-bucket` | (required) | S3 bucket for raw trades (or set `S3_BUCKET` env var). |
| `--s3-prefix` | `schemahub/raw_coinbase_trades` | S3 key prefix. |
| `--full-refresh` | false | Reset checkpoint and backfill from trade 1. Requires product argument. |
| `--checkpoint-s3` | false | Store checkpoints in S3 (default: local `state/` dir). |
| `--workers` | 2 | Concurrent product workers (1-10). |
| `--chunk-concurrency` | 15 | Parallel chunks per product (1-25). |
| `--dry-run` | false | Show what would be ingested without fetching. |

**Examples:**

```bash
# Ingest all products from seed (incremental, resumes from checkpoint)
python3 -m schemahub.cli ingest --s3-bucket my-bucket

# Ingest single product
python3 -m schemahub.cli ingest BTC-USD --s3-bucket my-bucket

# Full refresh for one product (backfill from trade 1)
python3 -m schemahub.cli ingest BTC-USD --s3-bucket my-bucket --full-refresh

# Production: S3 checkpoints, optimal parallelism
python3 -m schemahub.cli ingest --s3-bucket my-bucket --checkpoint-s3 --workers 5 --chunk-concurrency 9

# Preview what would be ingested
python3 -m schemahub.cli ingest --s3-bucket my-bucket --dry-run
```

**Total Threads:** `workers × chunk_concurrency`
- Default: 2 × 15 = 30 threads
- Optimal: 5 × 9 = 45 threads (saturates rate limit)
- Maximum: 10 × 25 = 250 threads

---

### `transform` — Raw JSONL to Unified Parquet

**Use case:** Transform raw JSONL files into unified Parquet format. Incremental by default (tracks processed files in manifest).

```bash
python3 -m schemahub.cli transform [OPTIONS]
```

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `--s3-bucket` | (required) | S3 bucket (or set `S3_BUCKET` env var). |
| `--raw-prefix` | `schemahub/raw_coinbase_trades` | S3 prefix for raw JSONL files. |
| `--unified-prefix` | `schemahub/unified_trades` | S3 prefix for Parquet output. |
| `--mapping-path` | (auto-detect) | Path to mapping YAML (optional). |
| `--rebuild` | false | Retransform ALL raw data to new version. |
| `--full-scan` | false | Run full dataset validation after transform. |

**Examples:**

```bash
# Incremental transform (only new raw files)
python3 -m schemahub.cli transform --s3-bucket my-bucket

# Full rebuild (reprocess all raw files)
python3 -m schemahub.cli transform --s3-bucket my-bucket --rebuild

# Transform with validation
python3 -m schemahub.cli transform --s3-bucket my-bucket --full-scan
```

---

### `update-seed` — Manage Product Seed File

**Use case:** Initialize or refresh the product seed file with available Coinbase products.

```bash
python3 -m schemahub.cli update-seed [OPTIONS]
```

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `--path` | `config/mappings/product_ids_seed.yaml` | Seed file path. |
| `--merge` | false | Merge with existing seed instead of replacing. |
| `--filter-regex` | (none) | Only keep product IDs matching regex (e.g., `.*-USD`). |
| `--dry-run` | false | Print what would be written without writing. |

**Examples:**

```bash
# Fetch all products and update seed
python3 -m schemahub.cli update-seed

# Fetch only USD pairs
python3 -m schemahub.cli update-seed --filter-regex '.*-USD'

# Merge new products with existing seed
python3 -m schemahub.cli update-seed --merge

# Preview changes
python3 -m schemahub.cli update-seed --dry-run
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | - | S3 bucket for all data (alternative to `--s3-bucket` flag) |
| `AWS_REGION` | `us-east-1` | AWS region for all services |
| `DYNAMODB_LOCKS_TABLE` | - | DynamoDB table for distributed locks (enables locking) |
| `LOCK_TTL_SECONDS` | 7200 | Lock TTL in seconds (2 hours default) |
| `DYNAMODB_HEALTH_TABLE` | - | DynamoDB table for exchange health tracking |
| `HEALTH_CHECK_ENABLED` | true | Enable/disable health checks |
| `CIRCUIT_BREAKER_ENABLED` | true | Enable/disable circuit breaker |
| `COINBASE_API_KEY` | - | Coinbase API key (for higher rate limits) |
| `COINBASE_API_SECRET` | - | Coinbase API secret |
| `ATHENA_DATABASE` | schemahub | Athena database name (for Lambda) |
| `ATHENA_WORKGROUP` | primary | Athena workgroup (for Lambda) |

---

## Configuration Constants

From `schemahub/config.py`:

### Rate Limiting

| Constant | Value | Description |
|----------|-------|-------------|
| `COINBASE_RATE_LIMIT_PUBLIC` | 8.0 | Requests/sec for public API |
| `COINBASE_RATE_LIMIT_AUTHENTICATED` | 8.0 | Requests/sec with API keys |
| `RATE_LIMITER_BURST_MULTIPLIER` | 1.5 | Burst allowance (1.5x steady rate) |

### Parallelism

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_PRODUCT_WORKERS` | 2 | Default concurrent products |
| `MAX_PRODUCT_WORKERS` | 10 | Maximum `--workers` value |
| `DEFAULT_CHUNK_CONCURRENCY` | 15 | Default parallel chunks per product |
| `MAX_CHUNK_CONCURRENCY` | 25 | Maximum `--chunk-concurrency` value |
| `DEFAULT_CHUNK_SIZE` | 1000 | Trades per API request |

### Locking

| Constant | Value | Description |
|----------|-------|-------------|
| `PRODUCT_LOCK_TTL_SECONDS` | 7200 | Per-product lock TTL (2 hours) |
| `PRODUCT_LOCK_RENEWAL_INTERVAL` | 1800 | Lock renewal interval (30 min) |
| `JOB_LOCK_TTL_SECONDS` | 14400 | Job-level lock TTL (4 hours) |

### Checkpointing

| Constant | Value | Description |
|----------|-------|-------------|
| `CHECKPOINT_BATCH_SIZE` | 5000 | Trades between checkpoint writes |
| `MIN_CHECKPOINT_INTERVAL_SECONDS` | 60 | Minimum time between checkpoints |

---

## Component Architecture

### 1. Orchestration Layer

```
EventBridge Scheduler (every 3-6 hours)
    ↓
ECS Task (Docker container)
    ↓
schemahub.cli commands
```

- **Stateless**: All state lives in S3 (checkpoints, data)
- **Ephemeral**: Containers spin up, run, terminate
- **Observable**: All logs → CloudWatch

---

### 2. Ingestion Pipeline

| Module | File | Purpose |
|--------|------|---------|
| **Connectors** | `schemahub/connectors/coinbase.py` | Exchange-specific API client (pagination, rate limits) |
| **Checkpoint Manager** | `schemahub/checkpoint.py` | Per-product watermark tracking (`last_trade_id` in S3) |
| **Lock Manager** | `schemahub/checkpoint.py` | DynamoDB distributed locks |
| **Raw Writer** | `schemahub/raw_writer.py` | Writes JSONL to S3 with deterministic keys |
| **Parallel Fetcher** | `schemahub/parallel.py` | Concurrent chunk fetching within products |
| **Progress Tracker** | `schemahub/progress.py` | Real-time progress bars and ETA |

**Watermark Pattern** (industry standard: Kafka, Spark, Kinesis):

```
1. Load checkpoint: last_trade_id = 12345
2. Fetch trades: after=12345 → get trades 12346..12500
3. Write to S3: deterministic key with timestamp + UUID
4. Update checkpoint: last_trade_id = 12500
5. Next run: resumes from 12500
```

**Cold Start Protection:**

On first run (no checkpoint), ingest uses a time cutoff (45 minutes) to prevent accidental full historical backfills.

---

### 3. Protection Layer

| Module | File | Purpose |
|--------|------|---------|
| **Rate Limiter** | `schemahub/rate_limiter.py` | Token bucket algorithm (8 req/sec) |
| **Health Service** | `schemahub/health.py` | Circuit breaker + health tracking |
| **Metrics Client** | `schemahub/metrics.py` | CloudWatch metrics publishing |

**Circuit Breaker States:**
- `CLOSED`: Normal operation, all requests proceed
- `OPEN`: API failing, all threads wait (5 min cooldown)
- `HALF_OPEN`: Testing recovery, one thread probes API

---

### 4. Transformation Pipeline

| Module | File | Purpose |
|--------|------|---------|
| **YAML Mappings** | `config/mappings/*.yaml` | Field mappings, type coercion rules |
| **Transform Engine** | `schemahub/transform.py` | Incremental processing, Pydantic validation |
| **Manifest Manager** | `schemahub/manifest.py` | Tracks processed files, transform history |
| **Validator** | `schemahub/validation.py` | Data quality gates (duplicates, freshness) |

**Transform Flow:**

```
Raw JSONL (exchange-specific fields)
    ↓ YAML mapping rules
Normalized records (unified schema)
    ↓ Pydantic validation
Parquet (partitioned by product_id)
    ↓ Manifest update
```

---

### 5. Query Layer

- **AWS Athena**: SQL over Parquet via Glue Data Catalog
- **Partitioning**: By `product_id` for efficient filtering
- **Format**: Snappy-compressed Parquet

---

### 6. Monitoring Layer

| Component | Purpose |
|-----------|---------|
| **CloudWatch Logs** | `/ecs/schemahub` - all container logs |
| **CloudWatch Metrics** | Ingest counts, error rates, data freshness |
| **CloudWatch Dashboard** | Real-time pipeline health visualization |
| **Data Quality Lambda** | Athena queries for health score, duplicates, gaps |

**Key Metrics:**

| Metric | Description |
|--------|-------------|
| `IngestCount` | Trades ingested per product |
| `TransformRecords` | Records transformed to Parquet |
| `DataAgeMinutes` | Minutes since last trade per product |
| `RateLimitErrors` | HTTP 429 errors |
| `CircuitBreakerState` | 0=closed, 0.5=half_open, 1=open |
| `OverallHealthScore` | Composite score (0-100) |

---

## Checkpoint & Lock Files

### Checkpoint Format

Local: `state/{product_id}.json`
S3: `{s3_prefix}/checkpoints/{product_id}.json`

```json
{
  "cursor": 12345678,
  "last_updated": "2025-12-15T10:30:45.123456Z"
}
```

### Lock Format (DynamoDB)

Table: `schemahub-locks`

| Attribute | Description |
|-----------|-------------|
| `lock_name` | Primary key (e.g., `product:coinbase:BTC-USD`) |
| `lock_id` | UUID of lock holder |
| `ttl` | Unix timestamp for auto-expiration |
| `acquired_at` | ISO timestamp |
| `holder` | Process identifier |

---

## Product Seed File

**Location:** `config/mappings/product_ids_seed.yaml`

```yaml
product_ids:
  - BTC-USD
  - ETH-USD
  - SOL-USD
metadata:
  source: coinbase
  last_updated: 2025-12-15T12:00:00Z
  count: 3
```

---

## S3 File Naming

### Raw JSONL

```
raw_coinbase_trades_{product}_{timestamp}_{run_id}_{first_id}_{last_id}_{count}.jsonl
```

Example: `raw_coinbase_trades_BTC-USD_20250122T120000Z_abc123_1000_2000_1000.jsonl`

### Unified Parquet

```
unified_trades_v{version}_{timestamp}_{batch}.parquet
```

Example: `unified_trades_v1_20250122T120000Z_001.parquet`

---

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=schemahub --cov-report=html

# Run specific test file
pytest tests/test_checkpoint.py -v

# Run specific test class
pytest tests/test_checkpoint.py::TestCheckpointManagerLocal -v

# Run tests matching a pattern
pytest tests/ -k "parallel" -v
```

### Test Modules

| Test File | Coverage |
|-----------|----------|
| `test_checkpoint.py` | CheckpointManager (local/S3), atomic writes |
| `test_seeds.py` | Seed file management, YAML parsing |
| `test_coinbase_connector_unit.py` | CoinbaseTrade, API pagination |
| `test_raw_writer_unit.py` | JSONL writing, datetime serialization |
| `test_rate_limiter.py` | Token bucket algorithm |
| `test_health.py` | Circuit breaker, health tracking |
| `test_parallel.py` | Parallel work queue, cursor targets |
| `test_manifest.py` | Manifest load/save, version tracking |
| `test_validation.py` | Data quality gates |
| `test_transform_integration.py` | JSONL → Parquet transformation |
| `test_integration_parallel.py` | Full pipeline with parallelism |
| `test_pipeline_invariants.py` | Correctness invariants |

### Test Dependencies

Tests use mocking to avoid external dependencies:

```bash
# Install dev dependencies
pip install -r requirements.txt

# No AWS credentials needed—tests use moto and botocore.Stubber
# No API keys needed—tests use mock HTTP responses
```

### Key Test Patterns

**S3 Mocking (moto):**
```python
@mock_aws
def test_write_to_s3():
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    # ... test code
```

**Determinism Tests:**
```python
def test_s3_key_deterministic():
    # Same inputs → same S3 key (idempotent writes)
    key1 = generate_key(product="BTC-USD", ts=ts, run_id=run_id)
    key2 = generate_key(product="BTC-USD", ts=ts, run_id=run_id)
    assert key1 == key2
```

**Invariant Tests:**
```python
def test_watermark_monotonic():
    # Checkpoint cursor never decreases
    assert new_cursor >= old_cursor
```
