# SchemaHub: Multi-Source Crypto Trades on Iceberg

## Table of Contents

- [SchemaHub Overview](#schemahub-overview)
- [System Architecture Diagrams](#system-architecture-diagrams)
- [Getting Started](#getting-started)
- [Ingestion Jobs](#ingestion-jobs)
- [Demos](#demos)
  - [Demo 1: Volatility Spike Replay](#demo-1-volatility-spike-replay)
  - [Demo 2: Live Discrepancy Detector](#demo-2-live-discrepancy-detector)
- [Project Goals](#project-goals)
- [High-Level Architecture](#high-level-architecture)
- [Storage & Catalog on AWS S3](#storage--catalog-on-aws-s3)
- [Data Model](#data-model)
- [Testing](#testing)
- [MVP Scope](#mvp-scope)
- [Possible Future Extensions](#possible-future-extensions)
- [Repository Layout](#repository-layout-suggested)

# SchemaHub Overview

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

- An Apache Iceberg–compatible engine (e.g. Spark with Iceberg).
- An S3 bucket to act as the Iceberg warehouse.
- (Optional but recommended) AWS Glue Data Catalog as the Iceberg catalog.
- API keys for:
  - Binance
  - Coinbase
  - Kraken
- A language runtime for the CLI + connectors (e.g. Python).

### Typical Local / Dev Flow

1. Configure AWS & exchange credentials

   ```bash
   export AWS_ACCESS_KEY_ID=...
   export AWS_SECRET_ACCESS_KEY=...
   export AWS_DEFAULT_REGION=...

   export BINANCE_API_KEY=...
   export BINANCE_API_SECRET=...
   export COINBASE_API_KEY=...
   export KRAKEN_API_KEY=...
   ```

2. Configure Iceberg catalog

   Set Spark / engine configs to point to:

   - Your S3 warehouse path.
   - Glue (or other catalog) as the Iceberg catalog.

3. Define YAML mappings

   Add one mapping file per source under `config/mappings/` as shown above.

4. Initialize Iceberg tables

   - Create `raw_*` tables in S3 via Iceberg DDL.
   - Create `trades_unified`.
   - Create meta tables (e.g., `schemahub_watermarks`).

5. Run ingestion + unification

   ```bash
   # Fetch last few minutes/hours of trades per source into raw tables
   schemahub run ingest binance coinbase

   # Transform new data into the unified table
   schemahub run unify trades_unified
   ```

6. Query the unified table

   ```sql
   SELECT exchange, symbol, price, quantity, trade_ts
   FROM schemahub.schemahub.trades_unified
   WHERE symbol = 'BTCUSDT'
   ORDER BY trade_ts DESC
   LIMIT 100;
   ```

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

---

### `backfill` — Bulk Historical Ingestion with Resume

**Use case:** Backfill historical data for one or more products, with fault tolerance and concurrent processing.

**Behavior:**
- Reads products from seed file.
- Fetches all available trades in chunks (default 100 per request).
- Processes multiple products concurrently via worker threads.
- Saves checkpoints after each chunk; supports `--resume` to continue from last trade ID.
- Stores raw JSONL files in S3, organized by product and timestamp.

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `--seed-path` | `config/mappings/product_ids_seed.yaml` | Product seed file. |
| `--chunk-size` | 100 | Trades per API request. |
| `--workers` | 1 | Concurrent product workers (e.g., 4 = process 4 products in parallel). |
| `--resume` | (false) | Load and resume from existing checkpoints. |
| `--checkpoint-s3` | (false) | Store checkpoints in S3; default is local `state/` dir. |
| `--s3-bucket` | (required) | S3 bucket for raw trades and checkpoints. |
| `--s3-prefix` | `schemahub/raw_coinbase_trades` | S3 key prefix. |
| `--dry-run` | (false) | Show plan without writing data. |

**How chunking & pagination work:**
- Coinbase API returns trades in descending order by trade ID (newest first).
- The `before` parameter is used as a cursor: "give me trades with trade_id < X".
- Each worker processes one product at a time, fetching chunks and writing to S3.
- After each chunk, a checkpoint is saved with `last_trade_id`, so resuming continues from there.
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

# Smaller chunks (more frequent checkpoint writes, useful for very large backfills)
python3 -m schemahub.cli backfill --s3-bucket my-bucket --workers 2 --chunk-size 50 --resume
```

**Checkpoint format (stored as JSON):**

Local: `state/{product_id}.json`  
S3: `{s3_prefix}/checkpoints/{product_id}.json`

```json
{
  "last_trade_id": 12345678,
  "trades_processed": 50000,
  "last_updated": "2025-12-15T10:30:45.123456Z"
}
```

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
| `--fetch` | (implicit) | Fetch from Coinbase API. |
| `--write` | (false) | Write to file (omit for dry-run). |
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
The CLI does not yet implement exponential backoff. If you hit rate limits, retry with a smaller `--workers` count or add delays between requests.

---

## Demos

### Demo 1: Volatility Spike Replay

Recreate real market events using your historical time-travel engine.

#### Demo Script

“Let’s replay BTC during a 12-second micro-volatility event yesterday.”

1. Select timestamp:  
   **2024-02-08 15:12:04 UTC**
2. Click **Replay**

A timeline slider animates the event tick-by-tick:

- Binance moves first  
- Coinbase lags by **40–120 ms**  
- Spreads widen  
- Cross-venue price disagreement spikes  

#### What This Demonstrates

- Real-time + historical coexistence  
- Precise time alignment across venues  
- Unified normalized schema  
- Iceberg/Parquet snapshot retrieval  
- Multi-venue merging and reconstruction  

This feels like a streamlined version of Bloomberg Terminal tick-by-tick playback, but built entirely on minimal infrastructure.

### Demo 2: Live Discrepancy Detector

Show real-time venue disagreement as it happens.

#### Demo Script

“I want to show you how exchanges disagree in real time.”

Your UI displays a continuously streaming table of normalized quotes:

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

## Possible Future Extensions

Not required for MVP, but nice stretch goals:

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
- More domains
  - Reuse the same pattern for e-commerce events, ad impressions, etc. Only the connectors, mappings, and target schema change.

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
