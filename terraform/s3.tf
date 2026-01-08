# -----------------------------------------------------------------------------
# S3 Bucket for SchemaHub Data
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "schemahub" {
  bucket        = var.s3_bucket_name
  force_destroy = var.s3_force_destroy

  tags = {
    Name = "${var.project_name}-data"
  }
}

resource "aws_s3_bucket_versioning" "schemahub" {
  bucket = aws_s3_bucket.schemahub.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "schemahub" {
  bucket = aws_s3_bucket.schemahub.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "schemahub" {
  bucket = aws_s3_bucket.schemahub.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rules for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "schemahub" {
  bucket = aws_s3_bucket.schemahub.id

  # Transition raw data to Infrequent Access after 30 days
  rule {
    id     = "raw-to-ia"
    status = "Enabled"

    filter {
      prefix = "schemahub/raw_"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Keep curated data in standard storage longer (frequently accessed)
  rule {
    id     = "curated-to-ia"
    status = "Enabled"

    filter {
      prefix = "schemahub/curated/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }

  # Clean up old checkpoint versions
  rule {
    id     = "checkpoint-cleanup"
    status = "Enabled"

    filter {
      prefix = "schemahub/checkpoints/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Create initial folder structure (optional - will be created on first write)
resource "aws_s3_object" "raw_folder" {
  bucket  = aws_s3_bucket.schemahub.id
  key     = "schemahub/raw_coinbase_trades/.keep"
  content = ""
}

resource "aws_s3_object" "curated_folder" {
  bucket  = aws_s3_bucket.schemahub.id
  key     = "schemahub/curated/.keep"
  content = ""
}

resource "aws_s3_object" "checkpoints_folder" {
  bucket  = aws_s3_bucket.schemahub.id
  key     = "schemahub/checkpoints/.keep"
  content = ""
}

# Athena query results bucket location
resource "aws_s3_object" "athena_results" {
  count   = var.create_athena_resources ? 1 : 0
  bucket  = aws_s3_bucket.schemahub.id
  key     = "athena-results/.keep"
  content = ""
}
