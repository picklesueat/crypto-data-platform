# SchemaHub: Multi-Source Crypto Trades on Iceberg

## Table of Contents

- [SchemaHub Overview](#schemahub-overview)
- [System Architecture Diagrams](#system-architecture-diagrams)
  - [Diagram 1](#diagram-1)
  - [Diagram 2](#diagram-2)
- [Demo 1: Volatility Spike Replay](#demo-1-volatility-spike-replay)
  - [Demo Script](#demo-script)
- [What This Demonstrates](#what-this-demonstrates)
- [Demo 2: Live Discrepancy Detector](#demo-2-live-discrepancy-detector)
- [Project Goals](#project-goals)
- [High-Level Architecture](#high-level-architecture)
  - [1. Source Connectors](#1-source-connectors)
  - [2. Raw Iceberg Tables](#2-raw-iceberg-tables)
  - [3. Schema Mapping Registry](#3-schema-mapping-registry)
  - [4. Unifier / Transformer](#4-unifier--transformer)
  - [5. Coordinator / CLI](#5-coordinator--cli)
- [Storage & Catalog on AWS S3](#storage--catalog-on-aws-s3)
  - [S3 Warehouse Layout](#s3-warehouse-layout)
  - [Example: Spark + AWS Glue Catalog](#example-spark--aws-glue-catalog)
  - [Table Naming Convention](#table-naming-convention)
  - [Example DDL for S3 Iceberg Tables](#example-ddl-for-s3-iceberg-tables)
  - [How the Pipeline Uses S3](#how-the-pipeline-uses-s3)
- [Data Model](#data-model)
  - [Raw Tables](#raw-tables)
  - [Unified Table](#unified-table)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Typical Local / Dev Flow](#typical-local--dev-flow)
- [MVP Scope](#mvp-scope)
- [Possible Future Extensions](#possible-future-extensions)
- [Repository Layout (Suggested)](#repository-layout-suggested)

# SchemaHub Overview

**SchemaHub** is a tiny, single-developer “data platform” for normalizing messy crypto exchange trade data into a unified **Apache Iceberg** table stored in **AWS S3**.

Think of it as a lightweight, personal **Fivetran + dbt + Iceberg** stack, purpose-built for **crypto exchanges**.

These demo flows are cinematic, high-impact, and require almost nothing beyond the minimal system. They turn your real-time + historical engine into something visual, intuitive, and impressive.


## System Architecture Diagrams

### Diagram 1
[Mermaid Diagram – POC](https://www.mermaidchart.com/app/projects/114c17aa-ed6a-40b6-baa8-f53cd0c5a982/diagrams/b71a0205-dfa0-43f1-8507-a1d41d7f0b44/share/invite/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb2N1bWVudElEIjoiYjcxYTAyMDUtZGZhMC00M2YxLTg1MDctYTFkNDFkN2YwYjQ0IiwiYWNjZXNzIjoiRWRpdCIsImlhdCI6MTc2NTQ5Mjg4MX0.VxMj1IkddSC41wgrfmwEHtIIBb4Rc_59VJRgqMLueYo)

### Diagram 2
[Mermaid Diagram – Final](https://www.mermaidchart.com/app/projects/114c17aa-ed6a-40b6-baa8-f53cd0c5a982/diagrams/dfb2ab18-7251-47f7-a07d-9c7679bab848/share/invite/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb2N1bWVudElEIjoiZGZiMmFiMTgtNzI1MS00N2Y3LWEwN2QtOWM3Njc5YmFiODQ4IiwiYWNjZXNzIjoiRWRpdCIsImlhdCI6MTc2NTQ5Mjk3N30.KOihCf-S_TLl8OkOKWW3W6icd1pBYwzJ05ESMsJu_Lk)

---

# Demo 1: Volatility Spike Replay

Recreate real market events using your historical time-travel engine.

## Demo Script

“Let’s replay BTC during a 12-second micro-volatility event yesterday.”

1. Select timestamp:  
   **2024-02-08 15:12:04 UTC**
2. Click **Replay**

A timeline slider animates the event tick-by-tick:

- Binance moves first  
- Coinbase lags by **40–120 ms**  
- Spreads widen  
- Cross-venue price disagreement spikes  

## What This Demonstrates

- Real-time + historical coexistence  
- Precise time alignment across venues  
- Unified normalized schema  
- Iceberg/Parquet snapshot retrieval  
- Multi-venue merging and reconstruction  

This feels like a streamlined version of Bloomberg Terminal tick-by-tick playback, but built entirely on minimal infrastructure.

---

# Demo 2: Live Discrepancy Detector

Show real-time venue disagreement as it happens.

## Demo Script

“I want to show you how exchanges disagree in real time.”

Your UI displays a continuously streaming table of normalized quotes:



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

## Repository Layout (Suggested)

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
