# -----------------------------------------------------------------------------
# CloudWatch Log Groups and Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "schemahub" {
  name              = "/ecs/${var.project_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-logs"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms (Optional)
# -----------------------------------------------------------------------------

# SNS Topic for alarm notifications
resource "aws_sns_topic" "alerts" {
  count = var.alarm_email != "" ? 1 : 0
  name  = "${var.project_name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Alarm: Ingest task failures
resource "aws_cloudwatch_metric_alarm" "ingest_failures" {
  count               = var.alarm_email != "" ? 1 : 0
  alarm_name          = "${var.project_name}-ingest-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "TaskFailure"
  namespace           = "ECS/ContainerInsights"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 2
  alarm_description   = "Ingest task has failed more than 2 times in an hour"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.schemahub.name
    ServiceName = "${var.project_name}-ingest"
  }

  alarm_actions = [aws_sns_topic.alerts[0].arn]
  ok_actions    = [aws_sns_topic.alerts[0].arn]

  tags = {
    Name = "${var.project_name}-ingest-failures-alarm"
  }
}

# Alarm: Transform task failures
resource "aws_cloudwatch_metric_alarm" "transform_failures" {
  count               = var.alarm_email != "" ? 1 : 0
  alarm_name          = "${var.project_name}-transform-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "TaskFailure"
  namespace           = "ECS/ContainerInsights"
  period              = 3600
  statistic           = "Sum"
  threshold           = 2
  alarm_description   = "Transform task has failed more than 2 times in an hour"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.schemahub.name
    ServiceName = "${var.project_name}-transform"
  }

  alarm_actions = [aws_sns_topic.alerts[0].arn]
  ok_actions    = [aws_sns_topic.alerts[0].arn]

  tags = {
    Name = "${var.project_name}-transform-failures-alarm"
  }
}

# -----------------------------------------------------------------------------
# Log Metric Filters (for custom metrics from logs)
# -----------------------------------------------------------------------------

# Metric filter for errors in logs
resource "aws_cloudwatch_log_metric_filter" "errors" {
  name           = "${var.project_name}-error-count"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.schemahub.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "SchemaHub"
    value     = "1"
  }
}

# Metric filter for trades ingested
resource "aws_cloudwatch_log_metric_filter" "trades_ingested" {
  name           = "${var.project_name}-trades-ingested"
  pattern        = "[timestamp, level=INFO, ..., msg=\"*trades*\", count]"
  log_group_name = aws_cloudwatch_log_group.schemahub.name

  metric_transformation {
    name          = "TradesIngested"
    namespace     = "SchemaHub"
    value         = "$count"
    default_value = "0"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "schemahub" {
  dashboard_name = "${var.project_name}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: Task Failures, Records Written, Exchange Status
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 6
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "IngestFailure", "Source", "coinbase", { stat = "Sum", label = "Ingest Failures" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Task Failures (24h)"
          period  = 300
          yAxis   = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 0
        width  = 6
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "IngestTotalTrades", "Source", "coinbase", { stat = "Sum", label = "Ingest" }],
            ["SchemaHub", "TransformRecords", { stat = "Sum", label = "Transform" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Records Written (24h)"
          period  = 300
          yAxis   = { left = { min = 0 } }
        }
      },
      {
        type   = "text"
        x      = 12
        y      = 0
        width  = 6
        height = 6
        properties = {
          markdown = "## 📊 Exchange Status\n\n| Exchange | Status |\n|----------|--------|\n| **Coinbase** | 🟢 Active |\n\n---\n\n**Schedule:** Every 3 hours\n\n**Tasks:**\n- Ingest (raw trades)\n- Transform (Parquet)"
        }
      },
      {
        type   = "log"
        x      = 18
        y      = 0
        width  = 6
        height = 6
        properties = {
          query  = <<-EOT
            SOURCE '/ecs/schemahub'
            | filter @message like /product_id|trades written|Ingested/
            | parse @message /product[_]?id[=:\s"']+(?<product>[A-Z0-9]+-[A-Z]+)/
            | filter ispresent(product)
            | stats max(@timestamp) as last_seen by product
            | sort last_seen desc
            | limit 30
          EOT
          region = var.aws_region
          title  = "Product Activity (Last Seen)"
          view   = "table"
        }
      },
      # Row 2: Trades Ingested by Product, Minutes Since Last Ingest
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "ProductIngestCount", "Product", "BTC-USD", { stat = "Sum", label = "BTC-USD" }],
            ["...", "ETH-USD", { stat = "Sum", label = "ETH-USD" }],
            ["...", "SOL-USD", { stat = "Sum", label = "SOL-USD" }],
            ["...", "DOGE-USD", { stat = "Sum", label = "DOGE-USD" }],
            ["...", "XRP-USD", { stat = "Sum", label = "XRP-USD" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Trades Ingested by Product (Top 5)"
          period  = 3600
          yAxis   = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "ProductLastIngest", "Product", "BTC-USD", { stat = "Maximum", label = "BTC-USD" }],
            ["...", "ETH-USD", { stat = "Maximum", label = "ETH-USD" }],
            ["...", "SOL-USD", { stat = "Maximum", label = "SOL-USD" }],
            ["...", "DOGE-USD", { stat = "Maximum", label = "DOGE-USD" }],
            ["...", "XRP-USD", { stat = "Maximum", label = "XRP-USD" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "Minutes Since Last Ingest (by Product)"
          period = 300
        }
      },
      # Row 3: Exchange Health Monitoring
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "ExchangeHealthy", "Source", "coinbase", { stat = "Average", label = "Coinbase API" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "Exchange API Health Status"
          period = 300
          annotations = {
            horizontal = [
              { value = 1, color = "#2ca02c", label = "Healthy" },
              { value = 0, color = "#d62728", label = "Unhealthy" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "ExchangeResponseTime", "Source", "coinbase", { stat = "p50", label = "p50 (median)" }],
            ["...", { stat = "p99", label = "p99" }],
            ["...", { stat = "Average", label = "Average" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "API Response Time (ms)"
          period  = 300
          yAxis   = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["SchemaHub", "ExchangeErrorRate", "Source", "coinbase", { stat = "Average", label = "Error Rate" }],
            [".", "CircuitBreakerState", ".", ".", { stat = "Average", label = "Circuit State", yAxis = "right" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Error Rate & Circuit Breaker State"
          period  = 300
          yAxis = {
            left  = { min = 0, max = 1, label = "Error Rate (0-1)" }
            right = { min = 0, max = 1, label = "Circuit (0=closed, 0.5=half_open, 1=open)" }
          }
          annotations = {
            horizontal = [
              { value = 0.1, color = "#ff9800", label = "Degraded Threshold" },
              { value = 0.3, color = "#d62728", label = "Unhealthy Threshold" }
            ]
          }
        }
      }
    ]
  })
}
