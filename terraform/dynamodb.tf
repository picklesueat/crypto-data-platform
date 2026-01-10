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
