# Correctness Guarantees & Invariants

## Overview

This document defines the core invariants that prevent data loss, duplication, and skipping in the ingestion pipeline. Understand these before modifying checkpoint logic, write paths, or failure recovery.

---

## Quick Reference: Key Guarantees

### Core Invariants at a Glance

| Invariant | Guarantee | Consequence |
|-----------|-----------|-------------|
| **Monotonic Watermark** | `last_trade_id` only increases or stays same | ✅ Zero data skipping; ✅ No data loss on retry |
| **Unique Trade Identity** | `(source, product_id, trade_id)` unique in curated | ✅ Duplicates absorbed at curated layer |
| **Unique S3 Keys** | Timestamp + UUID in every key | ✅ Global uniqueness even if runs happen same second |

### Write Semantics Summary

| Aspect | Current |
|--------|---------|
| **Raw layer** | Deterministic keys with UUID; retries overwrite (idempotent) |
| **Dedup strategy** | "Dedupe in curated" via Iceberg primary key |
| **Idempotency** | ✅ Fully idempotent: retries use same run_id → same S3 key |
| **Duplication risk** | **Accepted in raw:** Iceberg absorbs at curated layer |
| **Skipping risk** | Zero (monotonic watermark) |
| **Data loss risk** | Low (watermark updated after write succeeds) |

### Duplication & Omission Tolerance

| Layer | Duplication | Omission |
|-------|-------------|----------|
| **Raw S3** | ✅ Tolerated (dedup later) | ❌ Not tolerated |
| **Curated Iceberg** | ❌ Must not happen | ❌ Must not happen |
| **Overall** | ✅ Safe (dedupe in curated) | ✅ Safe (monotonic watermark) |

### Failure Recovery: Worst Case

| Scenario | Crash Point | Recovery | Risk |
|----------|-------------|----------|------|
| **Worst** | After S3 write, before checkpoint | Re-fetch same trades → overlap | ⚠️ Low (Iceberg dedup) |
| **Good** | Before S3 write | Retry writes same key | ✅ Safe |
| **Good** | After checkpoint update | No re-fetch | ✅ Safe |

---

## Table of Contents

- [Quick Reference: Key Guarantees](#quick-reference-key-guarantees)
  - [Core Invariants at a Glance](#core-invariants-at-a-glance)
  - [Write Semantics Summary](#write-semantics-summary)
  - [Duplication & Omission Tolerance](#duplication--omission-tolerance)
  - [Failure Recovery: Worst Case](#failure-recovery-worst-case)
- [Core Invariants](#core-invariants)
  - [1. Monotonic Watermark](#1-monotonic-watermark-per-product)
  - [2. Unique Trade Identity](#2-unique-trade-identity-iceberg-primary-key)
  - [3. Timestamp-Ordered S3 Keys](#3-timestamp-ordered-s3-keys-with-unique-run-ids)
- [Critical Assumptions](#critical-assumptions-a--d)
  - [Assumption A: Iceberg Primary Key Enforcement](#assumption-a-iceberg-primary-key-enforcement)
  - [Assumption B: Watermark Monotonicity](#assumption-b-watermark-monotonicity)
  - [Assumption C: Coinbase Trade IDs Monotonic](#assumption-c-coinbase-trade-ids-are-monotonic)
  - [Assumption D.1: Run ID Uniqueness](#assumption-d1-run-id-uniqueness)
  - [Assumption D.2: No Concurrent Ingests](#assumption-d2-no-concurrent-ingests-for-same-product)
- [Failure Analysis](#failure-analysis-d1d4)
  - [D1. Worst Possible Crash Point](#d1-worst-possible-crash-point)
  - [D2. Exact Recovery Behavior](#d2-exact-recovery-behavior-step-by-step)
  - [D3. Duplication & Omission Tolerance](#d3-duplication--omission-tolerance-1)
  - [D4. Silent Failure Detection](#d4-silent-failure-detection)
- [Deduplication Strategy](#deduplication-strategy-raw-vs-curated)
  - [Raw Layer: Duplicates Accepted](#raw-layer-duplicates-accepted)
  - [Curated Layer: Duplicates Must Be Zero](#curated-layer-duplicates-must-be-zero)
  - [Watermark: Best-Effort Optimization](#watermark-best-effort-optimization)
  - [Time-Based Cutoff (90 Minutes)](#time-based-cutoff-90-minutes)
- [Checkpoint Semantics](#checkpoint-semantics)
- [Production Readiness](#production-readiness)

---

## Core Invariants

### 1. Monotonic Watermark (Per-Product)
**Definition:** `last_trade_id` only increases or stays constant; never decreases.

**Why:** 
- Checkpoint updated *after* successful S3 write + successful Iceberg insert
- If either fails, checkpoint not updated → watermark unchanged
- Next run resumes from same watermark

**Consequence:** 
- ✅ No data skipping (watermark never jumps backward)
- ✅ No data loss (failed writes don't advance watermark)
- ⚠️ Possible re-fetching of same trades on retry (absorbed by Iceberg dedupe)

---

### 2. Unique Trade Identity (Iceberg Primary Key)
**Definition:** `(source, product_id, trade_id)` is unique in curated table.

**Why:**
- Coinbase: `trade_id` is globally unique per product, but only within Coinbase
- When adding other sources (Kraken, Binance, etc.), trade_id reuse is possible across exchanges
- Primary key must include `_source` to disambiguate

**Consequence:**
- ✅ Duplicate raw records from same source are absorbed at curated layer
- ✅ Retries that re-fetch same trades don't pollute curated layer
- ✅ Different exchanges can have overlapping trade IDs (dedup still works)
- ⚠️ **Assumes Iceberg primary key constraint is actually enforced**

**Deduplication Strategy: "Dedupe in Curated"**
- Raw layer: Duplicates may be written (by design)
- Curated layer: Iceberg's primary key enforces `(source, product_id, trade_id)` uniqueness
- Watermark acts as optimization: skips re-fetching data we've already seen

---

### 3. Timestamp-Ordered S3 Keys (with Unique Run IDs)
**Definition:** S3 object keys include `ingest_ts` (second-level granularity) AND `run_id` (UUID). Each run has globally unique key.

**Why:**
- Ingest key: `raw_coinbase_trades_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}.jsonl`
- Backfill key: `raw_coinbase_trades_{product}_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}_{first_id}_{last_id}_{count}.jsonl`
- `run_id` = UUID generated at start of each ingest/backfill session

**Consequence:**
- ✅ Keys are globally unique even if runs happen in the same second
- ✅ Retries of same run use same `run_id` → same S3 key → overwrites old object (idempotent)
- ✅ New runs always have new `run_id` → new S3 key → never collision

---

## Critical Assumptions (A–D)

### Assumption A: Iceberg Primary Key Enforcement
**What:** Iceberg's primary key on `(source, product_id, trade_id)` is actually enforced (not just documented).

**Implementation:**
- Upsert-on-write: New records with same key replace old ones
- Or merge-on-read: Queries see only latest value per key
- Constraint is part of table schema, not application logic

**If violated:** 
- 🔴 Raw overlaps propagate to curated → duplicate trades in analytics → wrong portfolio values

**Mitigation:**
- Before production: Insert duplicate key intentionally, verify only one record exists
- Document: Which Iceberg config enables this (e.g., primary key spec, merge behavior)
- Test: Add integration test for duplicate handling

---

### Assumption B: Watermark Monotonicity
**What:** Checkpoint file is written atomically (never partially written or corrupted).

**Current implementation:**
- Write to temp file, then atomic `os.replace()` (safe on Linux/macOS)
- S3 put_object is atomic (no partial uploads)

**If violated:**
- 🔴 Checkpoint could be half-written → unrecoverable state or duplicate fetch

**Mitigation:**
- Already done: Atomic writes via temp file + rename
- Validate: On load, ensure `last_trade_id` parses and is an integer

---

### Assumption C: Coinbase Trade IDs Are Monotonic
**What:** Within a product, `trade_id` increases monotonically (no gaps, no reordering).

**Current implementation:**
- Watermark assumes: `trade_id = 12345` means all trades `≤ 12345` have been fetched
- Next run fetches: `after=12345` → gets trades `> 12345`

**If violated:**
- ⚠️ If API returns trades out-of-order or with gaps, watermark might miss some
- Example: Fetch until trade_id=100, then later discover trade_id=99 exists

**Mitigation:**
- Best-effort: Watermark is optimization, not safety guarantee
- Safety net: Iceberg dedup catches any re-fetched data
- Assumption: Coinbase is well-engineered and doesn't violate this (fair bet)

---

### Assumption D.1: Run ID Uniqueness
**What:** UUID generated at start of each run is globally unique (collision probability ≈ 0).

**Current implementation:**
- `run_id = str(uuid.uuid4())` generated once per ingest/backfill session
- Included in all S3 keys for that run

**Consequence:**
- ✅ Retries within same run use same `run_id` → same S3 key → idempotent writes
- ✅ Different runs have different `run_id` → never collision even if runs start in same second

---

### Assumption D.2: No Concurrent Ingests for Same Product
**What:** Only one ingest job runs per product at a time.

**Current implementation:**
- No locking mechanism
- If two jobs run concurrently, both may read same checkpoint → both fetch same trades → duplicate writes

**If violated:**
- ⚠️ Race condition: Both jobs write to same S3 key (later one wins) → data loss
- ⚠️ Both jobs update checkpoint (later one wins) → other job's watermark clobbered

**Mitigation:**
- Operating constraint: Schedule jobs so only one runs per product
- Future: Add distributed locking (DynamoDB, S3 conditional write, etc.)
- For MVP: Document this assumption clearly

---


## Failure Analysis (D1–D4)

### D1. Worst Possible Crash Point

**WORST CASE:** After S3 write completes, but before checkpoint update.

**Code flow (ingest command):**
```
try:
    key, last_trade_id = ingest_coinbase(...)  # Includes S3 write ✅
    [CRASH HERE] ⚠️ 
    checkpoint_mgr.save(pid, checkpoint_data)  # Never happens ❌
```

**Why this is worst:**
- Raw S3 object written successfully (duplication possible on retry)
- Checkpoint NOT updated (watermark frozen at old value)
- Next run re-fetches same trades → overlapping raw payloads
- Iceberg dedup absorbs overlap, but raw layer wastes storage

**Less severe scenarios:**
- Crash before S3 write: Safe (retry writes same key)
- Crash after checkpoint update: Safe (watermark advanced, no re-fetch)

---

### D2. Exact Recovery Behavior (Step-by-Step)

When job resumes after crashing between S3 write and checkpoint update:

```
1. Load checkpoint: watermark frozen at last_trade_id=12345
2. Call ingest_coinbase(after=12345)
3. API returns trades > 12345 (e.g., 12346..12400)
4. Write to S3 with same key (same ingest_ts) → overlaps old payload
5. Update checkpoint: last_trade_id=12400
```

**Consequence:** Raw S3 object contains overlapping records (12346..12400 + potentially 12346..12400 again).

---

### D3. Duplication & Omission Tolerance

| Layer | Duplication | Omission | Current Status |
|-------|-------------|----------|---|
| **Raw S3** | ✅ Tolerated | ❌ Not tolerated | Possible on retry overlap |
| **Curated Iceberg** | ❌ Must not happen | ❌ Must not happen | Iceberg primary key enforces this |
| **Overall** | ✅ Acceptable (dedupe in curated) | ⚠️ Zero risk (monotonic watermark) | Safe for MVP |

**Why omission is impossible:**
- Monotonic watermark prevents skipping
- Next run always resumes from `last_trade_id`, never jumps ahead

**Why duplication in raw is acceptable:**
- Iceberg enforces `(trade_id, product_id, _source)` uniqueness
- Overlapping raw records absorbed at curated layer

---

### D4. Silent Failure Detection

**Current liveness signals: NONE (detect gaps manually)**

Recommended monitoring (when deployed):

1. **"Checkpoint advanced"** (hourly check)
   - Alert if no checkpoint update for 4+ hours (2x scheduled run interval)
   - Threshold: Last update > 4 hours ago

2. **"Raw objects written"** (per product per day)
   - Alert if 0 objects written in 24 hours for a product
   - Threshold: Expect ≥ 1 object/day per product

3. **"Trades ingested"** (CloudWatch metric, post-deployment)
   - Alert if avg trades/hour drops by >50% vs baseline
   - Threshold: TBD after observing baseline traffic

**Cannot use: "API calls succeeded but returned 0"**
- Dangerous: During low-volume periods, legitimate to get 0 new trades
- Could cause false alarms → noise → ignored alerts

**What to avoid:**
- Don't alert on "0 trades ingested" (could be normal)
- Do alert on "checkpoint never advanced" (always indicates a problem)

---

## Deduplication Strategy: Raw vs. Curated

### Raw Layer: Duplicates Accepted
The raw S3 layer is **not deduplicated**. Duplicates may occur:
- Retry after crash: Same `ingest_ts` → same S3 key → overwrites old object (payload may overlap)
- Multiple sources: Same product ID can exist in different per-source raw tables

**Example:** `raw_coinbase_trades_20251218T143022Z.jsonl` may contain:
```
{trade_id: 1001, product_id: BTC-USD, _source: coinbase, ...}
{trade_id: 1002, product_id: BTC-USD, _source: coinbase, ...}
{trade_id: 1001, product_id: BTC-USD, _source: coinbase, ...}  ← Duplicate from retry
```

### Curated Layer: Duplicates Must Be Zero
Iceberg enforces the contract:
```sql
-- Unique constraint on curated table
CREATE TABLE trades_unified (
    source VARCHAR,
    product_id VARCHAR,
    trade_id BIGINT,
    ...
    PRIMARY KEY (source, product_id, trade_id)
)
```

When processing raw data, Iceberg deduplicates:
```
Raw input:  {1001, BTC-USD, coinbase}, {1002, ...}, {1001, BTC-USD, coinbase}
Iceberg:    {1001, BTC-USD, coinbase}, {1002, ...}  ← Only one record per key
```

### Watermark: Best-Effort Optimization
The checkpoint watermark (`last_trade_id` per product) prevents re-fetching:
- Next run: `fetch(after=12345)` → gets trades > 12345 only
- **Best-effort:** Assumes Coinbase trade IDs are monotonic and gapless
- **Not guaranteed:** If API has quirks (gaps, out-of-order), watermark might miss data
- **Mitigation:** Iceberg dedup catches any re-fetched data anyway

### Time-Based Cutoff (90 Minutes)
Each ingest run stops fetching trades older than 90 minutes:
- **Why 90 minutes:** Provides 1.5x overlap with typical hourly schedule (runs every 45-60 min)
- **Benefit:** Avoids gaps between runs due to clock skew, delayed API responses, or scheduling jitter
- **Example:** If run A fetches trades from T-90min to now, and run B starts 45min later, B will re-fetch overlapping trades (T-45min to now), creating overlap that Iceberg deduplicates
- **No false negatives:** With 90min cutoff + hourly scheduling, all trades are fetched at least once (often multiple times, but dedup absorbs this)

---

## Checkpoint Semantics

### What is a Checkpoint?
A per-product marker: "I have successfully written all trades ≤ `last_trade_id` to Iceberg."

### Key Properties
- **Per-product:** Each product has independent watermark
- **Per-source:** Coinbase trades tracked separately from other sources (when added)
- **Monotonic:** Only increases or stays same
- **Atomic:** Written to S3 after both raw write AND curated insert succeed

### Recovery Behavior
1. Load checkpoint: `last_trade_id = 12345`
2. Next API call: `fetch(after=12345)` → gets trades `> 12345`
3. No re-fetching of `≤ 12345` (monotonicity guarantee)
4. No skipping of trades near watermark (API cursor is inclusive of boundary)

---

## Production Readiness

### Must-Have (Before Production)
- [ ] Verify Iceberg primary key is actually enforced (test!)
- [ ] Document all three invariants in code comments
- [ ] Add integration test: intentionally crash mid-ingest, verify no duplication

### Nice-to-Have (Post-POC)
- [ ] Include watermark in S3 key for true idempotency: `raw_{product}_{after}_{ingest_ts}.jsonl`
- [ ] Atomic S3 transactions: make checkpoint + data write a single operation
- [ ] Monitoring: Alert if duplicates detected in curated table
- [ ] Validation: Pre-ingest sanity check on checkpoint monotonicity

---

## Code Change Checklist

When modifying ingestion, checkpoint, or failure paths, verify:

- [ ] Does my change preserve monotonic watermark?
- [ ] Could it create overlapping S3 writes?
- [ ] Does it rely on Iceberg's primary key?
- [ ] Have I tested crash recovery?
- [ ] Is checkpoint written atomically?

