# Lambda function to run Athena data quality queries and publish metrics to CloudWatch

# IAM role for Lambda
resource "aws_iam_role" "data_quality_lambda" {
  count = var.create_athena_resources ? 1 : 0
  name  = "schemahub-data-quality-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "schemahub-data-quality-lambda-role"
  }
}

# Lambda policy for Athena, S3, CloudWatch, and Glue access
resource "aws_iam_role_policy" "data_quality_lambda" {
  count = var.create_athena_resources ? 1 : 0
  name  = "schemahub-data-quality-lambda-policy"
  role  = aws_iam_role.data_quality_lambda[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution"
        ]
        Resource = [
          "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/schemahub"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.schemahub.arn,
          "${aws_s3_bucket.schemahub.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/schemahub",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/schemahub/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}

# Lambda function
resource "aws_lambda_function" "data_quality" {
  count         = var.create_athena_resources ? 1 : 0
  filename      = data.archive_file.data_quality_lambda[0].output_path
  function_name = "schemahub-data-quality"
  role          = aws_iam_role.data_quality_lambda[0].arn
  handler       = "index.handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 1769  # 1 full vCPU

  source_code_hash = data.archive_file.data_quality_lambda[0].output_base64sha256

  environment {
    variables = {
      ATHENA_DATABASE    = "schemahub"
      ATHENA_WORKGROUP   = "schemahub"
      ATHENA_OUTPUT_PATH = "s3://${aws_s3_bucket.schemahub.id}/athena-results/"
      S3_BUCKET          = aws_s3_bucket.schemahub.id
      CURATED_PREFIX     = "schemahub/unified_trades/v1/"
    }
  }

  tags = {
    Name = "schemahub-data-quality-lambda"
  }
}

# Lambda source code
data "archive_file" "data_quality_lambda" {
  count       = var.create_athena_resources ? 1 : 0
  type        = "zip"
  output_path = "${path.module}/lambda_data_quality.zip"

  source {
    content  = <<-EOF
import boto3
import os
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

athena = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')
s3 = boto3.client('s3')

DATABASE = os.environ['ATHENA_DATABASE']
WORKGROUP = os.environ['ATHENA_WORKGROUP']
OUTPUT_PATH = os.environ['ATHENA_OUTPUT_PATH']
S3_BUCKET = os.environ.get('S3_BUCKET', '')
CURATED_PREFIX = os.environ.get('CURATED_PREFIX', 'schemahub/curated/')

# All queries defined upfront for parallel execution
QUERIES = {
    'overview': """
        SELECT
            symbol,
            COUNT(*) as total_records,
            SUM(price * quantity) as total_volume,
            MIN(trade_ts) as earliest_trade,
            MAX(trade_ts) as latest_trade
        FROM curated_trades
        GROUP BY symbol
        ORDER BY total_records DESC
    """,
    'demo_stats': """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as products,
            ROUND(SUM(price * quantity), 2) as total_volume_usd,
            MIN(trade_ts) as first_trade,
            MAX(trade_ts) as latest_trade,
            date_diff('day', MIN(trade_ts), MAX(trade_ts)) as data_span_days
        FROM curated_trades
    """,
    'freshness': """
        SELECT
            symbol,
            MAX(trade_ts) as last_trade_time,
            date_diff('minute', MAX(trade_ts), current_timestamp) as minutes_since_last_trade
        FROM curated_trades
        GROUP BY symbol
        ORDER BY minutes_since_last_trade DESC
    """,
    'gaps': """
        WITH trade_gaps AS (
            SELECT
                symbol,
                trade_ts,
                LAG(trade_ts) OVER (PARTITION BY symbol ORDER BY trade_ts) as prev_time,
                date_diff('second', LAG(trade_ts) OVER (PARTITION BY symbol ORDER BY trade_ts), trade_ts) as gap_seconds
            FROM curated_trades
        ),
        product_stats AS (
            SELECT
                symbol,
                AVG(gap_seconds) as avg_gap,
                STDDEV(gap_seconds) as stddev_gap,
                COUNT(*) as gap_count
            FROM trade_gaps
            WHERE gap_seconds IS NOT NULL AND gap_seconds > 0
            GROUP BY symbol
        ),
        anomalies AS (
            SELECT
                tg.symbol,
                tg.gap_seconds,
                ps.avg_gap,
                ps.stddev_gap,
                (tg.gap_seconds - ps.avg_gap) / NULLIF(ps.stddev_gap, 0) as z_score
            FROM trade_gaps tg
            JOIN product_stats ps ON tg.symbol = ps.symbol
            WHERE tg.gap_seconds IS NOT NULL
        )
        SELECT
            symbol,
            COUNT(*) as total_gaps,
            SUM(CASE WHEN z_score > 3 THEN 1 ELSE 0 END) as warning_gaps,
            SUM(CASE WHEN z_score > 4 THEN 1 ELSE 0 END) as severe_gaps,
            SUM(CASE WHEN z_score > 5 THEN 1 ELSE 0 END) as extreme_gaps
        FROM anomalies
        GROUP BY symbol
        ORDER BY extreme_gaps DESC, severe_gaps DESC
    """,
    'duplicates': """
        SELECT
            symbol,
            COUNT(*) - COUNT(DISTINCT trade_id) as duplicate_count
        FROM curated_trades
        GROUP BY symbol
        HAVING COUNT(*) > COUNT(DISTINCT trade_id)
    """,
    'daily': """
        SELECT COUNT(*) as records_today
        FROM curated_trades
        WHERE trade_ts >= current_date - interval '1' day
    """
}

def start_query(query_name, query_sql):
    """Start an Athena query and return the execution ID"""
    response = athena.start_query_execution(
        QueryString=query_sql,
        QueryExecutionContext={'Database': DATABASE},
        WorkGroup=WORKGROUP
    )
    query_id = response['QueryExecutionId']
    print(f"Started query '{query_name}' with ID: {query_id}")
    return query_name, query_id

def wait_and_get_results(query_name, query_id):
    """Wait for query to complete and return results"""
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(0.5)  # Poll every 500ms

    if state != 'SUCCEEDED':
        error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query '{query_name}' failed: {error}")
        return query_name, None

    results = athena.get_query_results(QueryExecutionId=query_id)
    print(f"Query '{query_name}' completed successfully")
    return query_name, results

def parse_results(results):
    """Parse Athena results into list of dicts"""
    if not results or 'ResultSet' not in results:
        return []

    rows = results['ResultSet']['Rows']
    if len(rows) < 2:
        return []

    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        values = [col.get('VarCharValue', '') for col in row['Data']]
        data.append(dict(zip(headers, values)))
    return data

def publish_metric(name, value, unit='Count', dimensions=None):
    """Publish metric to CloudWatch"""
    metric_data = {
        'MetricName': name,
        'Value': float(value) if value else 0,
        'Unit': unit,
        'Timestamp': datetime.utcnow()
    }
    if dimensions:
        metric_data['Dimensions'] = dimensions

    cloudwatch.put_metric_data(
        Namespace='SchemaHub/DataQuality',
        MetricData=[metric_data]
    )
    print(f"Published metric {name}={value}")

def get_parquet_size_gb(bucket, prefix):
    """Calculate total size of data files in curated layer."""
    if not bucket:
        return 0.0
    total_bytes = 0
    paginator = s3.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/') and not key.endswith('.json') and not key.endswith('.keep'):
                    total_bytes += obj['Size']
    except Exception as e:
        print(f"Error calculating S3 size: {e}")
        return 0.0
    return round(total_bytes / (1024 ** 3), 3)

def handler(event, context):
    """Main Lambda handler - runs all Athena queries in parallel"""
    start_time = time.time()
    results_summary = {}

    # Phase 1: Start all queries in parallel
    print("Phase 1: Starting all queries in parallel...")
    query_ids = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(start_query, name, sql)
            for name, sql in QUERIES.items()
        ]
        for future in as_completed(futures):
            name, qid = future.result()
            query_ids[name] = qid

    print(f"All {len(query_ids)} queries started in {time.time() - start_time:.1f}s")

    # Phase 2: Wait for all queries and collect results in parallel
    print("Phase 2: Waiting for query results...")
    raw_results = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(wait_and_get_results, name, qid)
            for name, qid in query_ids.items()
        ]
        for future in as_completed(futures):
            name, results = future.result()
            raw_results[name] = parse_results(results)

    print(f"All queries completed in {time.time() - start_time:.1f}s")

    # Phase 3: Process results and publish metrics (runs while S3 size is calculated)
    # Start S3 size calculation in background
    with ThreadPoolExecutor(max_workers=1) as executor:
        s3_future = executor.submit(get_parquet_size_gb, S3_BUCKET, CURATED_PREFIX)

        # Process overview results
        overview = raw_results.get('overview', [])
        results_summary['overview'] = overview
        total_records = sum(int(r.get('total_records', 0)) for r in overview)
        publish_metric('TotalRecords', total_records)
        publish_metric('ProductCount', len(overview))

        for row in overview:
            product = row.get('symbol', 'unknown')
            publish_metric('RecordsPerProduct', row.get('total_records', 0),
                          dimensions=[{'Name': 'ProductId', 'Value': product}])

        # Process demo stats
        demo_stats = raw_results.get('demo_stats', [])
        if demo_stats:
            stats = demo_stats[0]
            total_volume_usd = float(stats.get('total_volume_usd', 0) or 0)
            data_span_days = int(stats.get('data_span_days', 0) or 0)
            avg_daily_growth = total_records / max(data_span_days, 1)

            publish_metric('TotalVolumeUSD', total_volume_usd, 'None')
            publish_metric('DataSpanDays', data_span_days, 'Count')
            publish_metric('AvgDailyGrowth', avg_daily_growth, 'Count')
            results_summary['demo_stats'] = {
                'total_volume_usd': total_volume_usd,
                'data_span_days': data_span_days,
                'avg_daily_growth': avg_daily_growth,
                'first_trade': stats.get('first_trade'),
                'latest_trade': stats.get('latest_trade')
            }

        # Process freshness results
        freshness = raw_results.get('freshness', [])
        results_summary['freshness'] = freshness
        stale_count = 0
        avg_freshness = 0

        if freshness:
            avg_freshness = sum(int(r.get('minutes_since_last_trade', 0)) for r in freshness) / len(freshness)
            max_freshness = max(int(r.get('minutes_since_last_trade', 0)) for r in freshness)
            publish_metric('AvgDataFreshnessMinutes', avg_freshness, 'Count')
            publish_metric('MaxDataFreshnessMinutes', max_freshness, 'Count')

            stale_count = sum(1 for r in freshness if int(r.get('minutes_since_last_trade', 0)) > 60)
            publish_metric('StaleProductCount', stale_count)

            for row in freshness:
                product = row.get('symbol', 'unknown')
                publish_metric('FreshnessMinutes', row.get('minutes_since_last_trade', 0),
                              dimensions=[{'Name': 'ProductId', 'Value': product}])

        # Process gap results
        gaps = raw_results.get('gaps', [])
        results_summary['gaps'] = gaps
        total_warning = sum(int(r.get('warning_gaps', 0)) for r in gaps)
        total_severe = sum(int(r.get('severe_gaps', 0)) for r in gaps)
        total_extreme = sum(int(r.get('extreme_gaps', 0)) for r in gaps)

        publish_metric('WarningGapsTotal', total_warning)
        publish_metric('SevereGapsTotal', total_severe)
        publish_metric('ExtremeGapsTotal', total_extreme)

        # Process duplicate results
        duplicates = raw_results.get('duplicates', [])
        results_summary['duplicates'] = duplicates
        total_duplicates = sum(int(r.get('duplicate_count', 0)) for r in duplicates)
        publish_metric('DuplicateTradesTotal', total_duplicates)
        publish_metric('ProductsWithDuplicates', len(duplicates))

        # Process daily results
        daily = raw_results.get('daily', [])
        records_today = int(daily[0].get('records_today', 0)) if daily else 0
        publish_metric('DailyRecordsWritten', records_today)
        results_summary['daily_records'] = records_today

        # Get S3 size result
        parquet_size_gb = s3_future.result()
        publish_metric('ParquetSizeGB', parquet_size_gb, 'Gigabytes')
        results_summary['parquet_size_gb'] = parquet_size_gb

    # Calculate health score
    health_score = 100
    if total_duplicates > 0:
        health_score -= min(20, total_duplicates)
    if total_extreme > 0:
        health_score -= min(30, total_extreme * 5)
    if total_severe > 10:
        health_score -= min(20, total_severe)
    if freshness:
        stale_pct = (stale_count / len(freshness)) * 100
        health_score -= min(30, stale_pct)

    health_score = max(0, health_score)
    publish_metric('OverallHealthScore', health_score)

    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.1f}s")

    # Log summary
    print(json.dumps({
        'timestamp': datetime.utcnow().isoformat(),
        'execution_time_seconds': round(total_time, 1),
        'total_records': total_records,
        'product_count': len(overview),
        'total_volume_usd': results_summary.get('demo_stats', {}).get('total_volume_usd', 0),
        'data_span_days': results_summary.get('demo_stats', {}).get('data_span_days', 0),
        'avg_daily_growth': results_summary.get('demo_stats', {}).get('avg_daily_growth', 0),
        'parquet_size_gb': results_summary.get('parquet_size_gb', 0),
        'avg_freshness_minutes': avg_freshness if freshness else 0,
        'stale_products': stale_count if freshness else 0,
        'warning_gaps': total_warning,
        'severe_gaps': total_severe,
        'extreme_gaps': total_extreme,
        'duplicates': total_duplicates,
        'daily_records_written': records_today,
        'health_score': health_score,
        'details': results_summary
    }, indent=2, default=str))

    return {
        'statusCode': 200,
        'body': json.dumps({
            'health_score': health_score,
            'total_records': total_records,
            'product_count': len(overview),
            'execution_time_seconds': round(total_time, 1),
            'details': results_summary
        }, default=str)
    }
EOF
    filename = "index.py"
  }
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "data_quality_lambda" {
  count             = var.create_athena_resources ? 1 : 0
  name              = "/aws/lambda/schemahub-data-quality"
  retention_in_days = 14

  tags = {
    Name = "schemahub-data-quality-lambda-logs"
  }
}

# EventBridge schedule to run every 24 hours
resource "aws_scheduler_schedule" "data_quality" {
  count       = var.create_athena_resources ? 1 : 0
  name        = "schemahub-data-quality-schedule"
  description = "Runs data quality checks every 24 hours"

  schedule_expression          = "rate(24 hours)"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.data_quality[0].arn
    role_arn = aws_iam_role.eventbridge_lambda[0].arn
  }

  state = var.enable_scheduling ? "ENABLED" : "DISABLED"
}

# IAM role for EventBridge to invoke Lambda
resource "aws_iam_role" "eventbridge_lambda" {
  count = var.create_athena_resources ? 1 : 0
  name  = "schemahub-eventbridge-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = {
    Name = "schemahub-eventbridge-lambda-role"
  }
}

resource "aws_iam_role_policy" "eventbridge_lambda" {
  count = var.create_athena_resources ? 1 : 0
  name  = "schemahub-eventbridge-lambda-policy"
  role  = aws_iam_role.eventbridge_lambda[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.data_quality[0].arn
      }
    ]
  })
}

# Allow EventBridge to invoke Lambda
resource "aws_lambda_permission" "eventbridge" {
  count         = var.create_athena_resources ? 1 : 0
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_quality[0].function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.data_quality[0].arn
}
