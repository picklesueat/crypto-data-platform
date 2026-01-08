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
  memory_size   = 256

  source_code_hash = data.archive_file.data_quality_lambda[0].output_base64sha256

  environment {
    variables = {
      ATHENA_DATABASE    = "schemahub"
      ATHENA_WORKGROUP   = "schemahub"
      ATHENA_OUTPUT_PATH = "s3://${aws_s3_bucket.schemahub.id}/athena-results/"
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

athena = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')

DATABASE = os.environ['ATHENA_DATABASE']
WORKGROUP = os.environ['ATHENA_WORKGROUP']
OUTPUT_PATH = os.environ['ATHENA_OUTPUT_PATH']

def run_query(query):
    """Execute Athena query and wait for results"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        WorkGroup=WORKGROUP
    )
    query_id = response['QueryExecutionId']
    
    # Wait for query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query failed: {error}")
        return None
    
    # Get results
    results = athena.get_query_results(QueryExecutionId=query_id)
    return results

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

def handler(event, context):
    """Main Lambda handler"""
    results_summary = {}
    
    # 1. Total records and volume by product
    overview_query = """
    SELECT 
        product_id,
        COUNT(*) as total_records,
        SUM(price * size) as total_volume,
        MIN(time) as earliest_trade,
        MAX(time) as latest_trade
    FROM curated_trades
    GROUP BY product_id
    ORDER BY total_records DESC
    """
    
    overview = parse_results(run_query(overview_query))
    results_summary['overview'] = overview
    
    total_records = sum(int(r.get('total_records', 0)) for r in overview)
    publish_metric('TotalRecords', total_records)
    publish_metric('ProductCount', len(overview))
    
    for row in overview:
        product = row.get('product_id', 'unknown')
        publish_metric('RecordsPerProduct', row.get('total_records', 0),
                      dimensions=[{'Name': 'ProductId', 'Value': product}])
    
    # 2. Data freshness
    freshness_query = """
    SELECT 
        product_id,
        MAX(time) as last_trade_time,
        date_diff('minute', MAX(time), current_timestamp) as minutes_since_last_trade
    FROM curated_trades
    GROUP BY product_id
    ORDER BY minutes_since_last_trade DESC
    """
    
    freshness = parse_results(run_query(freshness_query))
    results_summary['freshness'] = freshness
    
    if freshness:
        avg_freshness = sum(int(r.get('minutes_since_last_trade', 0)) for r in freshness) / len(freshness)
        max_freshness = max(int(r.get('minutes_since_last_trade', 0)) for r in freshness)
        publish_metric('AvgDataFreshnessMinutes', avg_freshness, 'Count')
        publish_metric('MaxDataFreshnessMinutes', max_freshness, 'Count')
        
        # Stale data count (>60 min)
        stale_count = sum(1 for r in freshness if int(r.get('minutes_since_last_trade', 0)) > 60)
        publish_metric('StaleProductCount', stale_count)
        
        for row in freshness:
            product = row.get('product_id', 'unknown')
            publish_metric('FreshnessMinutes', row.get('minutes_since_last_trade', 0),
                          dimensions=[{'Name': 'ProductId', 'Value': product}])
    
    # 3. Gap detection (anomalous gaps)
    gap_query = """
    WITH trade_gaps AS (
        SELECT 
            product_id,
            time,
            LAG(time) OVER (PARTITION BY product_id ORDER BY time) as prev_time,
            date_diff('second', LAG(time) OVER (PARTITION BY product_id ORDER BY time), time) as gap_seconds
        FROM curated_trades
    ),
    product_stats AS (
        SELECT 
            product_id,
            AVG(gap_seconds) as avg_gap,
            STDDEV(gap_seconds) as stddev_gap,
            COUNT(*) as gap_count
        FROM trade_gaps
        WHERE gap_seconds IS NOT NULL AND gap_seconds > 0
        GROUP BY product_id
    ),
    anomalies AS (
        SELECT 
            tg.product_id,
            tg.gap_seconds,
            ps.avg_gap,
            ps.stddev_gap,
            (tg.gap_seconds - ps.avg_gap) / NULLIF(ps.stddev_gap, 0) as z_score
        FROM trade_gaps tg
        JOIN product_stats ps ON tg.product_id = ps.product_id
        WHERE tg.gap_seconds IS NOT NULL
    )
    SELECT 
        product_id,
        COUNT(*) as total_gaps,
        SUM(CASE WHEN z_score > 3 THEN 1 ELSE 0 END) as warning_gaps,
        SUM(CASE WHEN z_score > 4 THEN 1 ELSE 0 END) as severe_gaps,
        SUM(CASE WHEN z_score > 5 THEN 1 ELSE 0 END) as extreme_gaps
    FROM anomalies
    GROUP BY product_id
    ORDER BY extreme_gaps DESC, severe_gaps DESC
    """
    
    gaps = parse_results(run_query(gap_query))
    results_summary['gaps'] = gaps
    
    total_warning = sum(int(r.get('warning_gaps', 0)) for r in gaps)
    total_severe = sum(int(r.get('severe_gaps', 0)) for r in gaps)
    total_extreme = sum(int(r.get('extreme_gaps', 0)) for r in gaps)
    
    publish_metric('WarningGapsTotal', total_warning)
    publish_metric('SevereGapsTotal', total_severe)
    publish_metric('ExtremeGapsTotal', total_extreme)
    
    # 4. Duplicate check
    duplicate_query = """
    SELECT 
        product_id,
        COUNT(*) - COUNT(DISTINCT trade_id) as duplicate_count
    FROM curated_trades
    GROUP BY product_id
    HAVING COUNT(*) > COUNT(DISTINCT trade_id)
    """
    
    duplicates = parse_results(run_query(duplicate_query))
    results_summary['duplicates'] = duplicates
    
    total_duplicates = sum(int(r.get('duplicate_count', 0)) for r in duplicates)
    publish_metric('DuplicateTradesTotal', total_duplicates)
    publish_metric('ProductsWithDuplicates', len(duplicates))
    
    # 5. Overall health score (0-100)
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
    
    # Log summary
    print(json.dumps({
        'timestamp': datetime.utcnow().isoformat(),
        'total_records': total_records,
        'product_count': len(overview),
        'avg_freshness_minutes': avg_freshness if freshness else 0,
        'stale_products': stale_count if freshness else 0,
        'warning_gaps': total_warning,
        'severe_gaps': total_severe,
        'extreme_gaps': total_extreme,
        'duplicates': total_duplicates,
        'health_score': health_score,
        'details': results_summary
    }, indent=2, default=str))
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'health_score': health_score,
            'total_records': total_records,
            'product_count': len(overview),
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
