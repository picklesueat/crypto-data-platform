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
- [What Success Looks Like](#what-success-looks-like)
- [Out of Scope](#out-of-scope)
- [Architecture](#architecture)
- [Tradeoffs / Design Notes](#tradeoffs--design-notes)

### 4. Additional Details
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



---

# 3. Key Technical Details and Design

## The Problem

There are many different sources of crypto data, from different exchanges:

**Schema chaos:** Every exchange uses different field names, timestamp formats, and data structures. Combining Coinbase + Binance data means writing custom parsers for each.

**No history:** APIs give you real-time data but not easy historical access. Run infrastructure 24/7 or lose data forever.

**Reliability is hard:** Without checkpoints you get duplicates or gaps. Retries are risky. Parallel ingestion causes race conditions.

**Expensive to DIY:** Building connectors, handling pagination, storing queryable data, managing infra—it adds up. Managed solutions (Fivetran, etc.) are pricey for individuals.

**Data is everything in crypto:** Without reliable historical data, you can't backtest strategies, analyze market patterns, or build meaningful analytics. Every gap or duplicate corrupts your analysis.

---

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

## Out of Scope

_[To be added]_

---

## Architecture

_[To be added]_

---

# 4. Additional Details

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



