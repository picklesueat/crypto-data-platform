# -----------------------------------------------------------------------------
# AWS Glue and Athena Resources for SQL Queries
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Glue Database
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "schemahub" {
  count = var.create_athena_resources ? 1 : 0
  name  = var.glue_database_name

  description = "SchemaHub crypto trade data"

  # Optional: Enable Lake Formation permissions
  # create_table_default_permission {
  #   permissions = ["SELECT"]
  # }
}

# -----------------------------------------------------------------------------
# Glue Table: Curated Trades (Parquet)
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "curated_trades" {
  count         = var.create_athena_resources ? 1 : 0
  name          = "curated_trades"
  database_name = aws_glue_catalog_database.schemahub[0].name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
    "classification"      = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket_name}/schemahub/curated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    # Schema matching the normalized trade format
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "trade_id"
      type = "bigint"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "size"
      type = "double"
    }
    columns {
      name = "side"
      type = "string"
    }
    columns {
      name = "time"
      type = "timestamp"
    }
    columns {
      name = "ingested_at"
      type = "timestamp"
    }
  }

  # Partition by product_id for efficient queries
  partition_keys {
    name = "product_id"
    type = "string"
  }
}

# -----------------------------------------------------------------------------
# Glue Crawler (Optional - for automatic schema discovery)
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_crawler" {
  count = var.create_athena_resources ? 1 : 0
  name  = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_crawler_service" {
  count      = var.create_athena_resources ? 1 : 0
  role       = aws_iam_role.glue_crawler[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_crawler_s3" {
  count = var.create_athena_resources ? 1 : 0
  name  = "${var.project_name}-glue-crawler-s3"
  role  = aws_iam_role.glue_crawler[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.schemahub.arn,
          "${aws_s3_bucket.schemahub.arn}/schemahub/curated/*"
        ]
      }
    ]
  })
}

resource "aws_glue_crawler" "curated" {
  count         = var.create_athena_resources ? 1 : 0
  name          = "${var.project_name}-curated-crawler"
  database_name = aws_glue_catalog_database.schemahub[0].name
  role          = aws_iam_role.glue_crawler[0].arn

  s3_target {
    path = "s3://${var.s3_bucket_name}/schemahub/curated/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  # Run on-demand or schedule as needed
  # schedule = "cron(0 6 * * ? *)"  # Daily at 6 AM UTC

  tags = {
    Name = "${var.project_name}-curated-crawler"
  }
}

# -----------------------------------------------------------------------------
# Athena Workgroup
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "schemahub" {
  count = var.create_athena_resources ? 1 : 0
  name  = var.project_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.s3_bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Name = "${var.project_name}-athena-workgroup"
  }
}

# -----------------------------------------------------------------------------
# Athena Named Queries (Useful pre-built queries)
# -----------------------------------------------------------------------------

resource "aws_athena_named_query" "sample_queries" {
  count     = var.create_athena_resources ? 1 : 0
  name      = "sample-trade-query"
  workgroup = aws_athena_workgroup.schemahub[0].id
  database  = aws_glue_catalog_database.schemahub[0].name
  query     = <<-EOT
    -- Sample query: Get latest 100 trades for BTC-USD
    SELECT 
      source,
      product_id,
      trade_id,
      price,
      size,
      side,
      time,
      price * size as volume_usd
    FROM curated_trades
    WHERE product_id = 'BTC-USD'
    ORDER BY time DESC
    LIMIT 100;
  EOT
}

resource "aws_athena_named_query" "volume_by_hour" {
  count     = var.create_athena_resources ? 1 : 0
  name      = "volume-by-hour"
  workgroup = aws_athena_workgroup.schemahub[0].id
  database  = aws_glue_catalog_database.schemahub[0].name
  query     = <<-EOT
    -- Hourly volume analysis
    SELECT 
      product_id,
      date_trunc('hour', time) as hour,
      COUNT(*) as trade_count,
      SUM(price * size) as total_volume_usd,
      AVG(price) as avg_price,
      MIN(price) as low,
      MAX(price) as high
    FROM curated_trades
    WHERE time >= current_date - interval '7' day
    GROUP BY product_id, date_trunc('hour', time)
    ORDER BY hour DESC;
  EOT
}

resource "aws_athena_named_query" "data_freshness" {
  count     = var.create_athena_resources ? 1 : 0
  name      = "data-freshness"
  workgroup = aws_athena_workgroup.schemahub[0].id
  database  = aws_glue_catalog_database.schemahub[0].name
  query     = <<-EOT
    -- Check data freshness per product
    SELECT 
      product_id,
      MAX(trade_id) as latest_trade_id,
      MAX(time) as latest_trade_time,
      COUNT(*) as total_trades,
      date_diff('minute', MAX(time), current_timestamp) as minutes_since_last_trade
    FROM curated_trades
    GROUP BY product_id
    ORDER BY minutes_since_last_trade DESC;
  EOT
}
