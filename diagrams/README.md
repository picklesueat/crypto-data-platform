# Architecture Diagrams

## Color Legend

| Color | AWS Service Category |
|-------|---------------------|
| ![#FF9900](https://via.placeholder.com/15/FF9900/000000?text=+) Orange | Compute (ECS, Lambda) |
| ![#3F8624](https://via.placeholder.com/15/3F8624/000000?text=+) Green | Storage (S3) |
| ![#527FFF](https://via.placeholder.com/15/527FFF/000000?text=+) Blue | Database (DynamoDB) |
| ![#8C4FFF](https://via.placeholder.com/15/8C4FFF/000000?text=+) Purple | Analytics (Athena, Glue) |
| ![#FF4F8B](https://via.placeholder.com/15/FF4F8B/000000?text=+) Pink | Management (EventBridge, CloudWatch) |
| ![#D45B07](https://via.placeholder.com/15/D45B07/000000?text=+) Dark Orange | Rate Limiting |
| ![#1E8900](https://via.placeholder.com/15/1E8900/000000?text=+) Dark Green | Health Checks |

---

## 1. AWS Architecture

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
        CUR[(S3 Curated<br/>Parquet)]
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

---

## 2. Data Flow (High Level)

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
        CUR[(S3 Curated<br/>Parquet)]
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

---

## 3. Detailed Architecture

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
        CUR[(S3 Curated<br/>Parquet)]
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

---

## 4. Protection Layer Detail

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

---

## 5. Checkpoint & Locking Detail

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

## Component Legend

| Component | Description |
|-----------|-------------|
| **workers × chunk_concurrency** | Total threads (e.g., 5×9=45) |
| **Little's Law** | C = λ × W → 15 req/s × 3s = 45 threads |
| **Token Bucket** | Rate limit: 10-15 requests/second |
| **Circuit Breaker** | Opens on repeated 429 errors |
| **Watermark** | `last_trade_id` per product for resumable ingestion |
