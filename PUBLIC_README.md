# SchemaHub: Crypto Data Unification

A production-grade mini data platform that normalizes multi-exchange crypto trade data into a single queryable table. See [Correctness Invariants](CORRECTNESS_INVARIANTS.md) for detailed guarantees.

---

## Table of Contents

### 1. Overview
- [Production Dashboard](#production-dashboard)
- [Key Features](#key-features)
- [Architecture at a Glance](#architecture-at-a-glance)

### 2. Getting Started
- [Quickstart](#quickstart)
- [Usage](#usage)
- [Configuration](#configuration)
### 3. Key Technical Details and Design
- [Architecture](#architecture)
- [Correctness & Reliability](#correctness--reliability)
- [Failure Modes & Recovery](#failure-modes--recovery)
- [Storage Layout](#storage-layout)
- [Out of Scope](#out-of-scope)

### 4. Additional Details
- [The Problem](#the-problem)
- [What Success Looks Like](#what-success-looks-like)
- [Deployment](#deployment)
- [Roadmap](#roadmap)
- [License / Credits](#license--credits)

---

## Demo

Live production dashboard running on AWS CloudWatch, updated every 3-6 hours via automated ECS schedules.

### Headline Stats
![Dashboard Header](assets/dashboard-header.png)

### Pipeline Operations
![Pipeline Operations](assets/dashboard-pipeline-ops.png)

### API Health & Reliability
![API Health](assets/dashboard-api-health.png)

### Data Completeness & Storage
![Data Completeness](assets/dashboard-data-completeness.png)

---

## Key Features

- ✅ **Multi-exchange support** — Unified schema across different exchanges (Coinbase, extensible to others)
- ✅ **Warehouse-ready output** — Parquet in S3 optimized for Snowflake COPY INTO ingestion
- ✅ **Multithreaded ingestion** — Parallel product workers + concurrent chunk fetching (configurable up to 200 threads)
- ✅ **Rate limiting & circuit breaker** — Automatic backoff prevents API throttling, graceful degradation
- ✅ **Cloud-deployed & monitored** — Running on AWS with operational dashboards, automated scheduling, and observability
- ✅ **Data quality monitoring** — Automated health checks, freshness alerts, duplicate detection via Lambda
- ✅ **Billions-scale** — Handles billions of trades across products
- ✅ **Zero data loss** — Monotonic watermarks guarantee no gaps or skipped trades
- ✅ **Deduplication ready** — Deterministic keys enable downstream deduplication
- ✅ **Idempotent & resumable** — Safe retries, resume from any failure point
- ✅ **Distributed locking** — DynamoDB-based locks prevent concurrent job conflicts
- ✅ **Cost-effective** — ~$10-15/month on AWS for full operation

---

## Architecture at a Glance

```mermaid
flowchart LR
    subgraph Source
        API[Coinbase API<br/>JSON]
    end

    subgraph Ingest
        ING[INGEST<br/>Fetch + Checkpoint]
    end

    subgraph Storage
        RAW[(S3 Raw<br/>JSONL)]
        CUR[(S3 Unified<br/>Parquet)]
    end

    subgraph Transform
        TRF[TRANSFORM<br/>YAML Mappings<br/>incremental]
    end

    subgraph Query
        ATH[ATHENA<br/>SQL Analytics]
    end

    CHK[(Checkpoint<br/>Watermark)]

    API -->|JSON| ING
    ING -->|JSONL| RAW
    RAW -->|read| TRF
    TRF -->|Parquet| CUR
    CUR -->|query| ATH

    ING -.->|save| CHK
    CHK -.->|load| ING

    style API fill:#232F3E,stroke:#232F3E,color:#fff
    style ING fill:#FF9900,stroke:#232F3E,color:#fff
    style RAW fill:#3F8624,stroke:#232F3E,color:#fff
    style CUR fill:#3F8624,stroke:#232F3E,color:#fff
    style TRF fill:#FF9900,stroke:#232F3E,color:#fff
    style ATH fill:#8C4FFF,stroke:#232F3E,color:#fff
    style CHK fill:#527FFF,stroke:#232F3E,color:#fff
```

**Data Flow:**
1. **Ingest**: Fetch trades from Coinbase API using watermark checkpoints
2. **Raw Storage**: Immutable JSONL written to S3
3. **Transform**: YAML-based mapping normalizes schemas (incremental)
4. **Unified Storage**: Parquet tables for analytics
5. **Query**: SQL via AWS Athena

See [Architecture](#architecture) for detailed AWS infrastructure and internal component diagrams.

---

# 2. Getting Started

## Quickstart

**Prerequisites:** AWS account, S3 bucket, Docker

```bash
# Ingest trades
docker run YOUR_ECR_IMAGE python3 -m schemahub.cli ingest --s3-bucket BUCKET

# Transform to Parquet
docker run YOUR_ECR_IMAGE python3 -m schemahub.cli transform --s3-bucket BUCKET
```

---

## Usage

```bash
# Setup product list (one-time)
python3 -m schemahub.cli update-seed

# Ingest trades (incremental)
python3 -m schemahub.cli ingest --s3-bucket BUCKET

# Transform to Parquet (incremental)
python3 -m schemahub.cli transform --s3-bucket BUCKET
```

**Flags:** `--dry-run`, `--workers N`, `--chunk-concurrency N`

---

## Configuration

**Environment:** `S3_BUCKET`, `AWS_REGION`, `DYNAMODB_LOCKS_TABLE` (optional)

**S3 structure:**
```
s3://bucket/schemahub/
  ├── raw_coinbase_trades/    # JSONL
  ├── unified_trades/         # Parquet
  └── checkpoints/            # Watermarks
```

---


# 3. Key Technical Details and Design

---

## Architecture

### System Overview

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| **Orchestration** | EventBridge + ECS | Schedule jobs every 6 hours, run containers |
| **Ingestion** | Connectors + ThreadPoolExecutor | Parallel workers, watermark checkpoints |
| **Protection** | Token Bucket + Circuit Breaker + DynamoDB | Rate limiting, backoff, distributed locks |
| **Storage** | S3 (Raw + Unified) | Immutable JSONL → queryable Parquet |
| **Transformation** | YAML Mappings + Transform Engine | Normalize schemas incrementally |
| **Query** | Athena + Glue | SQL analytics over Parquet |
| **Observability** | CloudWatch + Lambda | Logs, metrics, dashboards, data quality |

### AWS Infrastructure

```mermaid
flowchart LR
    subgraph External
        CB[Coinbase REST API]
    end

    subgraph Orchestration
        EB[EventBridge<br/>every 6 hours]
    end

    subgraph ECS Fargate
        ING[ECS Ingest]
        TRF[ECS Transform<br/>incremental]
    end

    subgraph S3 Data Lake
        RAW[(S3 Raw<br/>JSONL)]
        CUR[(S3 Unified<br/>Parquet)]
        CHK[(Checkpoints)]
    end

    subgraph Analytics
        GLU[Glue Catalog]
        ATH[Athena<br/>SQL Queries]
    end

    subgraph Monitoring
        LAM[Lambda<br/>Data Quality]
        CW[CloudWatch<br/>Dashboards]
    end

    DDB[(DynamoDB<br/>Locks)]

    EB -->|triggers| ING
    EB -->|triggers| TRF
    CB -->|fetch trades| ING
    ING -->|writes JSONL| RAW
    ING -->|save/load| CHK
    ING -->|acquire lock| DDB
    RAW -.->|reads| TRF
    TRF -->|writes Parquet| CUR
    CUR --> GLU
    GLU --> ATH
    LAM -->|queries| ATH
    LAM -->|metrics| CW
    ING -->|logs| CW
    TRF -->|logs| CW

    style CB fill:#232F3E,stroke:#232F3E,color:#fff
    style EB fill:#FF4F8B,stroke:#232F3E,color:#fff
    style ING fill:#FF9900,stroke:#232F3E,color:#fff
    style TRF fill:#FF9900,stroke:#232F3E,color:#fff
    style RAW fill:#3F8624,stroke:#232F3E,color:#fff
    style CUR fill:#3F8624,stroke:#232F3E,color:#fff
    style CHK fill:#3F8624,stroke:#232F3E,color:#fff
    style GLU fill:#8C4FFF,stroke:#232F3E,color:#fff
    style ATH fill:#8C4FFF,stroke:#232F3E,color:#fff
    style LAM fill:#FF9900,stroke:#232F3E,color:#fff
    style CW fill:#FF4F8B,stroke:#232F3E,color:#fff
    style DDB fill:#527FFF,stroke:#232F3E,color:#fff
```

### Detailed Internal Architecture

```mermaid
flowchart TB
    subgraph External
        CB[Coinbase REST API<br/>1000 trades/req, 10 req/sec]
    end

    subgraph PROT[Protection Layer]
        PL[Rate Limiting<br/>+ Health Service]
    end

    subgraph PROC_LOCK[Process Lock]
        PRL[Per-Process Lock<br/>one per ECS task]
    end

    subgraph INGEST_PIPELINE[INGEST PIPELINE]
        subgraph Threading[Thread Pool Executor]
            subgraph BTC[BTC-USD Product]
                QBTC[Chunk Queue<br/>time ranges]
                BTC1[Thread 1]
                BTC2[Thread 2]
            end
        end
        subgraph OtherProducts[Other Products]
            ETHX[ETH-USD, etc.<br/>same as BTC]
        end
    end

    subgraph STATE[Per-Product State]
        PDL[Per-Product Lock]
        ST[Watermark Checkpoint]
    end

    subgraph TRANSFORM[TRANSFORM PIPELINE]
        TRF[Transform Reader<br/>multithreaded]
        TC[(Cursor: processed<br/>file names)]
    end

    subgraph STORAGE
        RAW[(S3 Raw<br/>JSONL)]
        CUR[(S3 Unified<br/>Parquet)]
    end

    subgraph MONITORING[DATA QUALITY]
        LAM[Lambda<br/>Health Checks]
        CW2[CloudWatch]
    end

    subgraph QUERY
        ATH[Athena]
        GLU[Glue Catalog]
    end

    %% Process lock acquired at startup
    INGEST_PIPELINE -.->|acquire at startup| PRL

    %% Workers get chunk index from queue
    QBTC -->|get next chunk index| BTC1
    QBTC -->|get next chunk index| BTC2

    %% REQUEST/RESPONSE through protection layer
    BTC1 ==>|request 1| PL
    BTC2 ==>|request 2| PL
    PL ==>|forward request| CB
    CB ==>|trades| BTC1 & BTC2

    %% Per-product locking and checkpoint (BTC only shown)
    BTC -->|sync state| STATE

    %% Workers write to storage
    BTC1 & BTC2 -->|write chunks| RAW

    %% Transform flow (incremental with file tracking)
    RAW --> TRF
    TRF -->|check processed| TC
    TC -->|skip seen files| TRF
    TRF -->|write Parquet| CUR
    CUR --> GLU
    GLU --> ATH

    %% Threads log to CloudWatch
    BTC1 --> CW2
    BTC2 --> CW2
    TRF --> CW2

    %% Monitoring
    LAM --> ATH
    LAM --> CW2

    style CB fill:#232F3E,stroke:#232F3E,color:#fff
    style PL fill:#D45B07,stroke:#232F3E,color:#fff
    style PRL fill:#527FFF,stroke:#232F3E,color:#fff
    style QBTC fill:#C925D1,stroke:#232F3E,color:#fff
    style BTC1 fill:#FF9900,stroke:#232F3E,color:#fff
    style BTC2 fill:#FF9900,stroke:#232F3E,color:#fff
    style ETHX fill:#666666,stroke:#232F3E,color:#fff
    style PDL fill:#527FFF,stroke:#232F3E,color:#fff
    style ST fill:#527FFF,stroke:#232F3E,color:#fff
    style TRF fill:#FF9900,stroke:#232F3E,color:#fff
    style TC fill:#527FFF,stroke:#232F3E,color:#fff
    style RAW fill:#3F8624,stroke:#232F3E,color:#fff
    style CUR fill:#3F8624,stroke:#232F3E,color:#fff
    style LAM fill:#FF9900,stroke:#232F3E,color:#fff
    style CW2 fill:#FF4F8B,stroke:#232F3E,color:#fff
    style ATH fill:#8C4FFF,stroke:#232F3E,color:#fff
    style GLU fill:#8C4FFF,stroke:#232F3E,color:#fff
```

#### Protection Layer Detail

```mermaid
flowchart LR
    subgraph Workers
        W[Worker Threads]
    end

    subgraph ProtectionLayer[Protection Layer]
        TKB[Token Bucket<br/>10-15 req/sec]
        HS[Health Service<br/>Circuit Breaker]
        BO[Exp. Backoff<br/>+ jitter]
    end

    subgraph External
        CB[Coinbase API]
    end

    %% Happy path
    W ==>|1. request token| TKB
    TKB ==>|2. granted| HS
    HS ==>|3. send request| CB
    CB ==>|4. return trades| W

    %% Error path
    CB -.->|on 429/5xx| HS
    HS -.->|trip circuit| BO
    BO -.->|wait + retry| TKB

    style W fill:#FF9900,stroke:#232F3E,color:#fff
    style TKB fill:#D45B07,stroke:#232F3E,color:#fff
    style HS fill:#1E8900,stroke:#232F3E,color:#fff
    style BO fill:#D45B07,stroke:#232F3E,color:#fff
    style CB fill:#232F3E,stroke:#232F3E,color:#fff
```

#### Checkpoint & Locking Detail

```mermaid
flowchart TB
    subgraph ECS_TASK[ECS Task / Process]
        PROC[Ingest Process]
    end

    subgraph ProductGroup[Per Product Group]
        PG[BTC, ETH, etc.]
    end

    subgraph PROC_LOCKING[Process-Level Lock]
        PRL[Per-Process Lock<br/>one per ECS task]
    end

    subgraph PRODUCT_LOCKING[Product-Level Locks]
        PDL[Per-Product Lock<br/>one per product]
    end

    subgraph LOCK_LIFECYCLE[Lock Lifecycle]
        ACQ[Acquire Lock<br/>conditional put]
        HB[Heartbeat<br/>refresh 30s]
        REL[Release Lock<br/>delete item]
    end

    subgraph Checkpoint[Checkpoint System]
        WM[(Ingest Watermark<br/>last_trade_id)]
        TC[(Transform Cursor<br/>processed file names)]
        AW[Atomic Write<br/>temp → rename]
        CHKS[(S3 Checkpoints)]
    end

    %% Process lock (once at startup)
    PROC -->|1. acquire at startup| PRL
    PRL --> ACQ

    %% Product lock (per product)
    PG -->|2. acquire per product| PDL
    PDL --> ACQ

    %% Lock lifecycle
    ACQ -->|3. hold| HB
    HB -->|4. release| REL

    %% Checkpoint flow
    PG -->|update| WM
    PG -->|update| TC
    WM --> AW
    TC --> AW
    AW --> CHKS

    style PROC fill:#FF9900,stroke:#232F3E,color:#fff
    style PG fill:#FF9900,stroke:#232F3E,color:#fff
    style PRL fill:#527FFF,stroke:#232F3E,color:#fff
    style PDL fill:#527FFF,stroke:#232F3E,color:#fff
    style ACQ fill:#527FFF,stroke:#232F3E,color:#fff
    style HB fill:#527FFF,stroke:#232F3E,color:#fff
    style REL fill:#527FFF,stroke:#232F3E,color:#fff
    style WM fill:#527FFF,stroke:#232F3E,color:#fff
    style TC fill:#527FFF,stroke:#232F3E,color:#fff
    style AW fill:#527FFF,stroke:#232F3E,color:#fff
    style CHKS fill:#3F8624,stroke:#232F3E,color:#fff
```

---

### Key Design Decisions

- **Watermark-based ingestion** — Incremental, resumable, no duplicates
- **Raw + Unified layers** — Immutable audit trail + flexible reprocessing
- **Deterministic S3 keys** — Idempotent writes; retries overwrite safely
- **Per-product checkpoints** — Parallel backfills without conflicts
- **YAML mappings** — Declarative, easy to extend for new sources
- **ECS over Lambda** — Longer timeouts, more memory for large batches
- **JSONL → Parquet** — Human-readable raw, 10x compressed analytics
- **Static thread pools** — Bottleneck is API rate limit, not local resources
- **Token bucket rate limiting** — Smooth request flow, respect API limits
- **Circuit breaker** — Back off on errors, prevent cascade failures

---

### Technologies

**Core:** Python 3.9+, Pydantic, PyArrow
**AWS:** ECS, EventBridge, S3, CloudWatch, Athena, Glue
**Format:** JSONL (raw), Parquet (unified)

---

### Concurrency & Performance

| Configuration | Threads | Use Case |
|---------------|---------|----------|
| `--workers 3 --chunk-concurrency 5` | 15 | Default |
| `--workers 5 --chunk-concurrency 9` | 45 | Optimal (saturates rate limit) |
| `--workers 10 --chunk-concurrency 20` | 200 | Backfill |

**Rate Limiting:** Token bucket (10-15 req/sec) + circuit breaker on repeated 429s.

See [Performance Guide](docs/PERFORMANCE.md) for Little's Law derivation, benchmarks, and tuning.

---

## Correctness & Reliability

| Guarantee | Mechanism |
|-----------|-----------|
| **Zero data loss** | Monotonic watermark checkpoints |
| **Zero duplicates** | Primary key dedup in unified layer |
| **Idempotent retries** | Deterministic S3 keys |
| **Resumable** | Per-product checkpoints |

See [CORRECTNESS_INVARIANTS.md](CORRECTNESS_INVARIANTS.md) for detailed analysis.

---

## Failure Modes & Recovery

| Scenario | Outcome |
|----------|---------|
| Job crashes mid-write | Retry from checkpoint, no data loss |
| Job crashes after write | Re-fetch overlaps, dedup absorbs |
| API rate limited | Circuit breaker backs off, retries |
| Network timeout | Exponential backoff with jitter |
| Concurrent jobs | DynamoDB lock prevents race conditions |
| Cold start (no checkpoint) | Time guard limits to recent data |

---

## Storage Layout

```
s3://{bucket}/schemahub/
├── raw_coinbase_trades/           # Immutable JSONL
│   └── {product}/{timestamp}_{uuid}.jsonl
├── unified_trades/                # Queryable Parquet
│   └── v{version}/
│       └── unified_trades_{timestamp}_{batch}.parquet
└── checkpoints/                   # Watermarks
    └── {product}.json
```

---

## Out of Scope

- Real-time streaming (batch-oriented by design)
- Order book / L2 data (trades only)
- Trading execution / portfolio management
- Multi-tenant SaaS (single-user deployment)

---

# 4. Additional Details

See [Component Details](docs/COMPONENT_DETAILS.md) for module-level documentation.

---

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

**Infrastructure:** Terraform in `terraform/` directory

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

**What gets created:**
- ECS Cluster + Task Definitions (ingest, transform)
- EventBridge Schedules (every 6 hours)
- S3 Bucket + Glue Database
- CloudWatch Dashboards + Alarms
- Lambda for data quality checks
- DynamoDB for distributed locking

**Cost:** ~$10-15/month for moderate usage

See [terraform/README.md](terraform/README.md) for full deployment guide.

### Cost Alerts

AWS Budgets alerts at configurable thresholds ($25, $50, $75...):

```hcl
# terraform.tfvars
billing_alert_email = "your-email@example.com"
billing_alert_threshold_increment = 25
```

---

## Roadmap

- [ ] Multi-exchange support (Binance, Kraken)
- [ ] Real-time streaming ingestion
- [ ] Iceberg table format for ACID transactions
- [ ] Backtest analytics dashboard

---

## License / Credits

MIT License

Built with: Python, AWS (ECS, S3, Athena, EventBridge), Terraform


---



