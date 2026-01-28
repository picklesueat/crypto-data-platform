# Component Details

## 1. Orchestration Layer

```
EventBridge Scheduler (every 6 hours)
    ↓
ECS Task (Docker container)
    ↓
schemahub.cli commands
```

- Stateless: All state lives in S3 (checkpoints, data)
- Ephemeral: Containers spin up, run, terminate
- Observable: All logs → CloudWatch

---

## 2. Ingestion Pipeline

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

---

## 3. Transformation Pipeline

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

---

## 4. Query Layer

- **AWS Athena**: SQL over Parquet via Glue Data Catalog
- **Partitioning**: By `product_id` for efficient filtering
- **Format**: Snappy-compressed Parquet
