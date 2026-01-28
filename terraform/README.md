# SchemaHub Terraform Deployment

This directory contains Terraform configuration to deploy the SchemaHub crypto data pipeline to AWS.

## Architecture Deployed

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  EventBridge    │────▶│    ECS/Fargate  │────▶│      S3         │
│  (45 min)       │     │  (ingest task)  │     │  (raw + curated)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
┌─────────────────┐     ┌─────────────────┐            ▼
│  EventBridge    │────▶│    ECS/Fargate  │     ┌─────────────────┐
│  (60 min)       │     │ (transform task)│     │  Athena/Glue    │
└─────────────────┘     └─────────────────┘     │  (SQL queries)  │
                                                └─────────────────┘
```

## Resources Created

| Resource | Purpose |
|----------|---------|
| **S3 Bucket** | Raw JSONL, curated Parquet, checkpoints |
| **ECR Repository** | Docker image storage |
| **ECS Cluster** | Fargate cluster for running tasks |
| **ECS Task Definitions** | Ingest, Transform, Full-Refresh configs |
| **EventBridge Schedules** | Automated 45-min ingest, 60-min transform |
| **IAM Roles** | Task role (S3 access), Execution role (ECR/logs) |
| **CloudWatch Log Group** | Centralized logging |
| **Glue Database/Table** | Athena schema definitions |
| **Athena Workgroup** | SQL query environment |

## Prerequisites

1. **AWS CLI** configured with appropriate credentials
2. **Terraform** >= 1.0
3. **Docker** for building and pushing images

## Quick Start

### 1. Initialize Terraform

```bash
cd terraform
terraform init
```

### 2. Configure Variables

```bash
# Copy example and edit
cp terraform.tfvars.example terraform.tfvars

# At minimum, set:
# s3_bucket_name = "your-unique-bucket-name"
```

### 3. Review Plan

```bash
terraform plan
```

### 4. Deploy Infrastructure

```bash
terraform apply
```

### 5. Build and Push Docker Image

After `terraform apply`, use the output commands:

```bash
# Get ECR login
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <ECR_URL>

# Build and push (from project root)
cd ..
docker build -t schemahub .
docker tag schemahub:latest <ECR_URL>:latest
docker push <ECR_URL>:latest
```

### 6. Run Initial Seed

```bash
# Run locally first to populate product_ids_seed.yaml
python3 -m schemahub.cli update-seed --fetch --write

# Commit the seed file, rebuild, and push image
```

### 7. Manual Task Execution

```bash
# Get the run commands from Terraform outputs
terraform output run_ingest_command
terraform output run_transform_command
terraform output run_full_refresh_command
```

## Configuration Options

### Required Variables

| Variable | Description |
|----------|-------------|
| `s3_bucket_name` | Globally unique S3 bucket name |

### Important Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `aws_region` | `us-east-1` | AWS region |
| `ingest_schedule` | `rate(6 hours)` | Ingest frequency |
| `transform_schedule` | `rate(6 hours)` | Transform frequency |
| `enable_scheduling` | `true` | Enable/disable automated runs |
| `create_athena_resources` | `true` | Create Glue/Athena resources |
| `ecs_cpu` | `512` | CPU units for ingest task |
| `ecs_memory` | `1024` | Memory (MiB) for ingest task |

### Secrets (Optional)

For authenticated Coinbase API access (higher rate limits):

1. Create secrets in AWS Secrets Manager
2. Set `coinbase_api_key_arn` and `coinbase_api_secret_arn`

## S3 Bucket Structure

```
s3://{bucket}/
├── schemahub/
│   ├── raw_coinbase_trades/     # Immutable JSONL
│   │   └── {product}/{ts}_{uuid}.jsonl
│   ├── curated/                  # Queryable Parquet
│   │   └── product_id={product}/
│   │       └── {ts}.parquet
│   └── checkpoints/              # Watermarks
│       └── {mode}/{product}.json
└── athena-results/               # Query outputs
```

## Cost Estimation

| Resource | Estimated Monthly Cost |
|----------|----------------------|
| ECS Fargate (ingest) | ~$3-5 |
| ECS Fargate (transform) | ~$2-4 |
| S3 Storage (100GB) | ~$2 |
| CloudWatch Logs | ~$1 |
| **Total** | **~$8-12/month** |

*Based on: 32 ingest runs/day, 24 transform runs/day, Fargate Spot pricing*

## Athena Queries

After deployment, query your data:

```sql
-- Connect to Athena workgroup: schemahub

-- Latest trades
SELECT * FROM curated_trades 
WHERE product_id = 'BTC-USD' 
ORDER BY time DESC LIMIT 100;

-- Hourly volume
SELECT 
  date_trunc('hour', time) as hour,
  COUNT(*) as trades,
  SUM(price * size) as volume_usd
FROM curated_trades
WHERE product_id = 'BTC-USD'
GROUP BY 1 ORDER BY 1 DESC;

-- Data freshness check
SELECT product_id, MAX(time), COUNT(*) 
FROM curated_trades 
GROUP BY product_id;
```

## Updating

```bash
# Pull latest code
git pull

# Rebuild and push Docker image
docker build -t schemahub .
docker push <ECR_URL>:latest

# If Terraform config changed
terraform plan
terraform apply
```

## Destroying

```bash
# WARNING: This will delete all data!
# First, enable force_destroy if bucket has data:
# s3_force_destroy = true

terraform destroy
```

## Troubleshooting

### Task Fails to Start

1. Check CloudWatch logs: `/ecs/schemahub`
2. Verify ECR image exists: `aws ecr describe-images --repository-name schemahub`
3. Check subnet has internet access (public IP or NAT gateway)

### No Data in Athena

1. Run Glue crawler: `aws glue start-crawler --name schemahub-curated-crawler`
2. Check S3 for Parquet files: `aws s3 ls s3://{bucket}/schemahub/curated/`
3. Verify table partitions: `MSCK REPAIR TABLE curated_trades;`

### Rate Limiting

Consider adding Coinbase API keys for higher rate limits. Create secrets in AWS Secrets Manager and configure the ARNs.

## Files

| File | Purpose |
|------|---------|
| `versions.tf` | Terraform and provider versions |
| `variables.tf` | Input variable definitions |
| `s3.tf` | S3 bucket, lifecycle rules |
| `ecr.tf` | ECR repository |
| `iam.tf` | IAM roles and policies |
| `ecs.tf` | ECS cluster, task definitions |
| `eventbridge.tf` | Scheduled triggers |
| `cloudwatch.tf` | Logs, metrics, alarms |
| `athena.tf` | Glue database, Athena workgroup |
| `outputs.tf` | Output values and helper commands |
