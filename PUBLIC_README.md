# Crypto Data Unification Project

Real operational, Mini Dataplatform, to Normalize real world multi-exchange crypto trade data into a single queryable table.

---

## Table of Contents

### 1. Overview
- [Demo](#demo)
- [Key Features](#key-features)
- [Architecture at a Glance](#architecture-at-a-glance)

### 2. Getting Started
- [Quickstart](#quickstart)
- [Usage](#usage)
- [Configuration](#configuration)
### 3. Key Technical Details and Design
- [The Problem](#the-problem)
- [Architecture](#architecture)
- [Correctness Guarantees](#correctness-guarantees)
- [Failure Modes & Recovery](#failure-modes--recovery)
- [Storage Layout](#storage-layout)
- [Out of Scope](#out-of-scope)
- [Tradeoffs / Design Notes](#tradeoffs--design-notes)

### 4. Additional Details
- [What Success Looks Like](#what-success-looks-like)
- [Deployment](#deployment)
- [Roadmap](#roadmap)
- [License / Credits](#license--credits)

---

## Demo

_[Add GIF/video/screenshots here]_
demo of DB? im not sure yes
_[Hosted link if available]_
live stats of data size / row count
coins / features up 

---

## Key Features

- ✅ **Multi-exchange support** — Unified schema across different exchanges (Coinbase, extensible to others)
- ✅ **SQL-queryable** — Parquet tables (works with DuckDB, Spark, Athena)
- ✅ **Simple deployment** — Single Docker image, no external databases, stateless compute
- ✅ **Cloud-deployed & monitored** — Running on AWS with operational dashboards, automated scheduling, and observability
- ✅ **Zero data loss** — Monotonic watermarks guarantee no gaps or skipped trades
- ✅ **Deduplication ready** — Deterministic keys enable downstream deduplication
- ✅ **Idempotent & resumable** — Safe retries, resume from any failure point

---

## Architecture at a Glance
![Diagram](image.png)
[View Interactive Architecture Diagram](https://www.mermaidchart.com/app/projects/114c17aa-ed6a-40b6-baa8-f53cd0c5a982/diagrams/dfb2ab18-7251-47f7-a07d-9c7679bab848/share/invite/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb2N1bWVudElEIjoiZGZiMmFiMTgtNzI1MS00N2Y3LWEwN2QtOWM3Njc5YmFiODQ4IiwiYWNjZXNzIjoiRWRpdCIsImlhdCI6MTc2NzgyNTU2M30._xpUWZLYaNlR636JN3bRb7nbpdlq5CRI7pKnAA4HbSI)

**Data Flow:**
1. **Scheduled Orchestration**: EventBridge triggers ECS tasks every 45 minutes
2. **Ingest**: Connectors fetch trades using watermark checkpoints
3. **Raw Storage**: Immutable JSONL written to S3 with deterministic keys
4. **Transform**: YAML-based mapping engine normalizes schemas
5. **Curated Storage**: Parquet tables partitioned by product_id
6. **Query**: SQL analytics via AWS Athena
7. **Monitor**: CloudWatch logs feed operational dashboards

---

# 2. Getting Started

## Quickstart

**Prerequisites:** AWS account, S3 bucket, ECR image, IAM role with S3 access

**Run containers:**
```bash
# Ingest
docker run YOUR_ECR_IMAGE python3 -m schemahub.cli ingest --s3-bucket BUCKET

# Transform
docker run YOUR_ECR_IMAGE python3 -m schemahub.cli transform --s3-bucket BUCKET
```

Automated scheduling: See [Deployment](#deployment)

---

## Usage

**Core commands:**
```bash
# Setup product list (one-time)
python3 -m schemahub.cli update-seed --fetch --write

# Ingest trades (incremental, watermark-based)
python3 -m schemahub.cli ingest --s3-bucket BUCKET

# Transform to Parquet (incremental)
python3 -m schemahub.cli transform --s3-bucket BUCKET
```

**Flags:** `--dry-run`, `--resume`, `--workers N`

---

## Configuration

**Environment:** `AWS_REGION`, `S3_BUCKET`

**YAML:** `config/mappings/product_ids_seed.yaml`, `coinbase_transform.yaml`

**S3 structure:**
```
s3://bucket/schemahub/
  ├── raw_coinbase_trades/    # JSONL
  ├── curated/                # Parquet
  └── checkpoints/            # Watermarks
```

---


# 3. Key Technical Details and Design

---

## Architecture

### System Overview

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| **Orchestration** | EventBridge + ECS | Schedule jobs every 45 min, run Docker containers |
| **Ingestion** | Connectors + Checkpoints | Fetch trades via API, track watermarks |
| **Storage** | S3 (Raw + Curated) | Immutable JSONL → queryable Parquet |
| **Transformation** | YAML Mappings + Transform Engine | Normalize schemas incrementally |
| **Query** | AWS Athena | SQL analytics over Parquet |
| **Monitoring** | CloudWatch | Logs, metrics, dashboards |

---

### Component Details

**1. Orchestration Layer**
```
EventBridge Scheduler (every 45 min)
    ↓
ECS Task (Docker container)
    ↓
schemahub.cli commands
```
- Stateless: All state lives in S3 (checkpoints, data)
- Ephemeral: Containers spin up, run, terminate
- Observable: All logs → CloudWatch

**2. Ingestion Pipeline**

| Module | File | Purpose |
|--------|------|---------|
| **Connectors** | `schemahub/connectors/coinbase.py` | Exchange-specific API client (pagination, rate limits) |
| **Checkpoint Manager** | `schemahub/checkpoint.py` | Per-product watermark tracking (`last_trade_id` in S3) |
| **Raw Writer** | `schemahub/raw_writer.py` | Writes JSONL to S3 with deterministic keys |

**Watermark Pattern** (industry standard: Kafka, Spark, Kinesis):
```
1. Load checkpoint: last_trade_id = 12345
2. Fetch trades: after=12345 → get trades 12346..12500
3. Write to S3: deterministic key with timestamp + UUID
4. Update checkpoint: last_trade_id = 12500
5. Next run: resumes from 12500
```

**3. Transformation Pipeline**

| Module | File | Purpose |
|--------|------|---------|
| **YAML Mappings** | `config/mappings/*.yaml` | Field mappings, type coercion rules |
| **Transform Engine** | `schemahub/transform.py` | Incremental processing, Pydantic validation |

**Transform Flow:**
```
Raw JSONL (exchange-specific fields)
    ↓ YAML mapping rules
Normalized records (unified schema)
    ↓ Pydantic validation
Parquet (partitioned by product_id)
```

**4. Storage Layer (S3)**

```
s3://{bucket}/schemahub/
├── raw_coinbase_trades/           # Raw layer (immutable)
│   └── {product}/{ts}_{uuid}.jsonl
├── curated/                        # Curated layer (queryable)
│   └── product_id={product}/
│       └── {ts}.parquet
└── checkpoints/                    # Watermarks
    └── {product}.json
```

**Design principle:** Raw = audit trail (never delete), Curated = analytics (can rebuild from raw)

**5. Query Layer**
- **AWS Athena**: SQL over Parquet via Glue Data Catalog
- **Partitioning**: By `product_id` for efficient time filtering
- **Format**: Snappy-compressed Parquet

---

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Watermark-based ingestion** | Incremental, resumable, no duplicates |
| **Raw + Curated layers** | Immutable audit trail + flexible reprocessing |
| **Deterministic S3 keys** | Idempotent writes; retries overwrite safely |
| **Per-product checkpoints** | Parallel backfills without conflicts |
| **YAML mappings** | Declarative, easy to extend for new sources |
| **ECS over Lambda** | Longer timeouts, more memory for large batches |

---

### Technologies

**Core:** Python 3.9+, Pydantic, PyArrow  
**AWS:** ECS, EventBridge, S3, CloudWatch, Athena, Glue  
**Format:** JSONL (raw), Parquet (curated)


---

## Correctness Guarantees

### Quick Reference

| Guarantee | Mechanism | Risk if Violated |
|-----------|-----------|------------------|
| **Zero data loss** | Monotonic watermark | Trades skipped |
| **Zero duplicates (curated)** | Primary key dedup | Wrong analytics, double-counted volume |
| **Idempotent retries** | Deterministic S3 keys | Duplicate files, wasted storage |
| **Resumable from any failure** | Per-product checkpoints | Manual intervention required |

---

### Core Invariants

**1. Unique Trade Identity**

Each trade is uniquely identified by `(source, product_id, trade_id)`:

| Field | Example | Purpose |
|-------|---------|---------|
| `source` | `coinbase` | Disambiguate across exchanges |
| `product_id` | `BTC-USD` | Trading pair |
| `trade_id` | `12345678` | Exchange-assigned unique ID |

**Why it matters:**
- This composite key is the foundation for everything else
- Watermarks track progress using `trade_id`
- Deduplication relies on this key being globally unique
- Coinbase `trade_id` is unique within Coinbase, but Binance may reuse same IDs
- Primary key must include `source` for multi-exchange support

---

**2. Monotonic Watermark**

The `last_trade_id` checkpoint only increases or stays the same—never decreases.

**Watermark type:** This is a **trade ID-based watermark** (not time-based). Coinbase trade IDs are monotonically increasing integers, making them ideal cursors.

```
Run 1: Fetch trades 1-100    → checkpoint = 100
Run 2: Fetch trades 101-200  → checkpoint = 200
Run 3: (crash before write)  → checkpoint = 200 (unchanged)
Run 4: Fetch trades 201-300  → checkpoint = 300
```

**Why trade ID over timestamp:**
- Trade IDs are guaranteed unique and sequential (per product)
- Timestamps can have duplicates (multiple trades same millisecond)
- API pagination uses `after={trade_id}` cursor natively
- No timezone or clock skew issues

**The 90-minute time guard:**

On cold start (no checkpoint), we don't want to backfill all history. The system fetches trades going backward in time but stops after 90 minutes:

```
First run (no checkpoint):
  → Fetch newest trades first (descending by trade_id)
  → Stop when trade timestamp > 90 min ago
  → Save checkpoint at highest trade_id seen
  
Subsequent runs (checkpoint exists):
  → Fetch trades after last_trade_id
  → Still apply 90-min guard as safety net
```

**Why 90 minutes (2x overlap):**
- Jobs scheduled every 45 minutes
- 90-min cutoff = 2x the schedule interval
- Provides generous overlap to handle scheduling jitter, delayed runs, or clock skew
- Worst case: trades fetched twice (dedup handles it)

**Why it matters:**
- Checkpoint updated *after* successful S3 write
- If write fails, checkpoint unchanged → retry from same point
- Guarantees: no trades skipped, safe to retry any failure

**Implementation:** `schemahub/checkpoint.py`
```python
# Checkpoint only advances after successful write
if write_succeeded:
    checkpoint_mgr.save(product_id, {"last_trade_id": max_trade_id})
```

---

**3. Deterministic S3 Keys**

Every S3 object key includes `{timestamp}_{run_id}`:

```
raw_coinbase_trades/BTC-USD/20251215T103045Z_a1b2c3d4.jsonl
                            ↑ timestamp      ↑ UUID per run
```

**Why it matters:**
- Same run → same `run_id` → same key → overwrites (idempotent)
- Different runs → different `run_id` → no collisions
- Retrying a failed job is always safe


**Implementation:** `schemahub/raw_writer.py`
```python
run_id = str(uuid.uuid4())  # Generated once per job
key = f"{prefix}/{product}/{timestamp}_{run_id}.jsonl"
```

---

### Deduplication Strategy

**Raw Layer: Duplicates Tolerated**
```
Retry after crash may re-fetch same trades:
  {trade_id: 1001, ...}
  {trade_id: 1002, ...}
  {trade_id: 1001, ...}  ← Duplicate from retry (acceptable)
```

**Curated Layer: Duplicates Eliminated**
```
After dedup processing:
  {trade_id: 1001, ...}  ← Only one record per (source, product_id, trade_id)
  {trade_id: 1002, ...}
```

**Why this design:**
- Raw layer = immutable audit trail (keep everything)
- Curated layer = analytics (must be deduplicated)
- Watermark = optimization (avoids re-fetching, but not safety-critical)

---

### Cold Start vs Steady State

| Mode | Checkpoint Exists? | Behavior |
|------|-------------------|----------|
| **Cold start** | No | Fetch last 45 min only, save checkpoint |
| **Steady state** | Yes | Fetch after `last_trade_id`, apply 45-min guard |
| **Backfill** | N/A | Separate command, fetches all historical data |

**Example timeline:**
```
12:00 - Run A (cold start): Fetches 11:15-12:00, checkpoint = trade_50000
12:45 - Run B: Fetches after trade_50000 (gets 12:00-12:45), checkpoint = trade_51000
13:30 - Run C: Fetches after trade_51000 (gets 12:45-13:30), checkpoint = trade_52000
```

No gaps, no duplicates in curated layer, safe cold starts.

---**
```
Run A (12:00): Fetches 11:15-12:00
Run B (12:45): Fetches 12:00-12:45
                       ↑ No gap between runs
```

---

### What This Means in Practice

| Scenario | Outcome |
|----------|---------|
| Job crashes mid-write | Safe: retry from checkpoint, no data loss |
| Job crashes after write, before checkpoint | Safe: re-fetch overlaps, dedup absorbs |
| Two jobs run concurrently (same product) | ⚠️ Race condition: avoid via scheduling |
| First run on new product | Safe: 45-min cutoff prevents full backfill |

---

### Assumptions & Constraints

| Assumption | If Violated |
|------------|-------------|
| Coinbase trade IDs are monotonic | Watermark may miss trades (dedup catches) |
| No concurrent ingests per product | Race condition on checkpoint |
| S3 put_object is atomic | Partial writes (mitigated by temp file + rename) |
| Checkpoint file is not corrupted | Validate on load; fail loud |

See [CORRECTNESS_INVARIANTS.md](CORRECTNESS_INVARIANTS.md) for exhaustive analysis.

---

## Failure Modes & Recovery

_[To be added]_

---

## Storage Layout

_[To be added]_

---

## Out of Scope

_[To be added]_

---

## Tradeoffs / Design Notes

_[To be added]_

---

# 4. Additional Details

## The Problem

There are many different sources of crypto data, from different exchanges:

**Schema chaos:** Every exchange uses different field names, timestamp formats, and data structures. Combining Coinbase + Binance data means writing custom parsers for each.

**No history:** APIs give you real-time data but not easy historical access. Run infrastructure 24/7 or lose data forever.

**Reliability is hard:** Without checkpoints you get duplicates or gaps. Retries are risky. Parallel ingestion causes race conditions.

**Expensive to DIY:** Building connectors, handling pagination, storing queryable data, managing infra—it adds up. Managed solutions (Fivetran, etc.) are pricey for individuals.

**Data is everything in crypto:** Without reliable historical data, you can't backtest strategies, analyze market patterns, or build meaningful analytics. Every gap or duplicate corrupts your analysis.


## What Success Looks Like

**Correctness:**
- Zero data loss (monotonic watermark, never skip trades)
- Deduplication ready (deterministic S3 keys enable downstream dedup)
- Idempotent retries (same run → same result)

**Performance:**
- Ingest: ~1-2 min for 30-min batch
- Backfill: 1M+ trades/hour per worker
- Queries: < 10 sec for 1B+ trades

**Cost:**
- ~$10 to backfill 100M trades
- ~$5/month for incremental updates (50 products, 30-min batches)
- 10x storage savings (Parquet vs JSON)

**UX:**
- < 10 min from zero to first query
- 3 CLI commands cover everything
- `--dry-run` flags for safety

**Operations:**
- No databases needed (checkpoints in S3)
- Stateless (works on AWS Glue/Lambda)
- One Docker image

---

## Deployment

_[To be added]_

---

## Tradeoffs / Design Notes

_[To be added]_

---

## Roadmap

_[To be added]_

---

## License / Credits

_[To be added]_


---



