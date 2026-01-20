# -----------------------------------------------------------------------------
# DynamoDB Table for Distributed Locking
# Prevents concurrent ingest/transform runs from stepping on each other
# -----------------------------------------------------------------------------

resource "aws_dynamodb_table" "locks" {
  name         = "${var.project_name}-locks"
  billing_mode = "PAY_PER_REQUEST"  # Pay per request - minimal cost for low-frequency locks
  hash_key     = "lock_name"

  attribute {
    name = "lock_name"
    type = "S"
  }

  # TTL automatically deletes expired locks (crash recovery)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Name = "${var.project_name}-locks"
  }
}

# Output for reference
output "dynamodb_locks_table" {
  description = "DynamoDB table name for distributed locks"
  value       = aws_dynamodb_table.locks.name
}

# -----------------------------------------------------------------------------
# DynamoDB Table for Exchange Health Tracking
# Stores circuit breaker state and health metrics for exchange APIs
# -----------------------------------------------------------------------------

resource "aws_dynamodb_table" "exchange_health" {
  name         = "${var.project_name}-exchange-health"
  billing_mode = "PAY_PER_REQUEST"  # Pay per request - low cost for health checks
  hash_key     = "exchange_name"
  range_key    = "timestamp"

  attribute {
    name = "exchange_name"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"  # ISO8601 timestamp string for easy sorting
  }

  # TTL automatically deletes old health records after 7 days
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Name = "${var.project_name}-exchange-health"
  }
}

# Output for reference
output "dynamodb_health_table" {
  description = "DynamoDB table name for exchange health tracking"
  value       = aws_dynamodb_table.exchange_health.name
}
