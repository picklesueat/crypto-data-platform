# Crypto Data Unification Project

Normalize multi-exchange crypto trade data into a single queryable table with zero data loss.

---

## Demo

_[Add GIF/video/screenshots here]_
demo of DB? im not sure yes
_[Hosted link if available]_

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

## Table of Contents

- [The Problem](#the-problem)
- [What Success Looks Like](#what-success-looks-like)
- [Out of Scope](#out-of-scope)
- [Quick Start](#quick-start)
- [More Docs](#more-docs)

---

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



---


## Quick Start

```bash
# 1. Get product list
python3 -m schemahub.cli update-seed --fetch --write

# 2. Ingest some data
python3 -m schemahub.cli ingest BTC-USD --s3-bucket YOUR_BUCKET

# 3. Query it
python3 -c "
import pandas as pd
import pyarrow.parquet as pq
import boto3

# Query Parquet files directly
df = pd.read_parquet('s3://YOUR_BUCKET/schemahub/transformed/product_id=BTC-USD/')
print(df.head())
"
```

Done in < 10 minutes.

See [README.md](README.md) for details.

---

## More Docs

- [README.md](README.md) — Full technical docs
- [CORRECTNESS_INVARIANTS.md](CORRECTNESS_INVARIANTS.md) — How we guarantee no data loss
- [LOGGING_GUIDE.md](LOGGING_GUIDE.md) — Debugging
