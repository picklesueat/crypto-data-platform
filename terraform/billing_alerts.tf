# -----------------------------------------------------------------------------
# AWS Billing Alerts
# Uses AWS Budgets to alert at configurable spending thresholds
# -----------------------------------------------------------------------------

locals {
  # Generate threshold list: $25, $50, $75, ... up to max
  billing_thresholds = [
    for i in range(1, floor(var.billing_alert_max_threshold / var.billing_alert_threshold_increment) + 1) :
    i * var.billing_alert_threshold_increment
  ]

  # Only create billing resources if at least one notification method is configured
  create_billing_alerts = var.billing_alert_email != "" || var.billing_alert_phone != ""
}

# -----------------------------------------------------------------------------
# SNS Topic for Billing Alerts
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "billing_alerts" {
  count = local.create_billing_alerts ? 1 : 0

  name = "${var.project_name}-billing-alerts"

  tags = {
    Name = "${var.project_name}-billing-alerts"
  }
}

# Policy allowing AWS Budgets to publish to the topic
resource "aws_sns_topic_policy" "billing_alerts" {
  count = local.create_billing_alerts ? 1 : 0

  arn = aws_sns_topic.billing_alerts[0].arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowBudgetsPublish"
        Effect = "Allow"
        Principal = {
          Service = "budgets.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.billing_alerts[0].arn
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# SNS Subscriptions
# -----------------------------------------------------------------------------

# Email subscription
resource "aws_sns_topic_subscription" "billing_email" {
  count = var.billing_alert_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.billing_alerts[0].arn
  protocol  = "email"
  endpoint  = var.billing_alert_email
}

# SMS subscription
# Note: Phone number must be in E.164 format (e.g., +18476130632)
# SMS delivery requires your AWS account to be out of the SMS sandbox for production use
resource "aws_sns_topic_subscription" "billing_sms" {
  count = var.billing_alert_phone != "" ? 1 : 0

  topic_arn = aws_sns_topic.billing_alerts[0].arn
  protocol  = "sms"
  endpoint  = var.billing_alert_phone
}

# -----------------------------------------------------------------------------
# AWS Budgets - One per threshold
# -----------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly_cost" {
  for_each = local.create_billing_alerts ? toset([for t in local.billing_thresholds : tostring(t)]) : toset([])

  name         = "${var.project_name}-monthly-${each.value}-usd"
  budget_type  = "COST"
  limit_amount = each.value
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_sns_topic_arns  = [aws_sns_topic.billing_alerts[0].arn]
  }

  # Also alert when forecasted to exceed
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_sns_topic_arns  = [aws_sns_topic.billing_alerts[0].arn]
  }

  tags = {
    Name = "${var.project_name}-budget-${each.value}"
  }
}
