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
  period              = 3600  # 1 hour
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
    name      = "TradesIngested"
    namespace = "SchemaHub"
    value     = "$count"
    default_value = "0"
  }
}
