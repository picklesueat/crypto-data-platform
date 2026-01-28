# -----------------------------------------------------------------------------
# General Configuration
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "schemahub"
}

# -----------------------------------------------------------------------------
# S3 Configuration
# -----------------------------------------------------------------------------

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage. Must be globally unique."
  type        = string
}

variable "s3_force_destroy" {
  description = "Allow Terraform to delete non-empty S3 bucket"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# ECS Configuration
# -----------------------------------------------------------------------------

variable "ecs_cpu" {
  description = "CPU units for ECS task (256, 512, 1024, 2048, 4096)"
  type        = number
  default     = 512
}

variable "ecs_memory" {
  description = "Memory (MiB) for ECS task"
  type        = number
  default     = 1024
}

variable "ecs_task_cpu_transform" {
  description = "CPU units for transform task (may need more than ingest)"
  type        = number
  default     = 1024
}

variable "ecs_task_memory_transform" {
  description = "Memory (MiB) for transform task"
  type        = number
  default     = 2048
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------

variable "vpc_id" {
  description = "VPC ID for ECS tasks. If not provided, uses default VPC."
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = "List of subnet IDs for ECS tasks. If empty, uses default subnets."
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = "Assign public IP to ECS tasks (required if no NAT gateway)"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Scheduling
# -----------------------------------------------------------------------------

variable "ingest_schedule" {
  description = "Cron or rate expression for ingest job (EventBridge format)"
  type        = string
  default     = "rate(6 hours)"
}

variable "transform_schedule" {
  description = "Cron or rate expression for transform job"
  type        = string
  default     = "rate(6 hours)"
}

variable "enable_scheduling" {
  description = "Enable EventBridge schedules (set false for manual runs only)"
  type        = bool
  default     = true
}

variable "data_quality_schedule" {
  description = "Cron or rate expression for data quality Lambda"
  type        = string
  default     = "rate(6 hours)"
}

# -----------------------------------------------------------------------------
# Secrets / Environment
# -----------------------------------------------------------------------------

variable "coinbase_api_key_arn" {
  description = "ARN of Secrets Manager secret containing Coinbase API key (optional)"
  type        = string
  default     = ""
}

variable "coinbase_api_secret_arn" {
  description = "ARN of Secrets Manager secret containing Coinbase API secret (optional)"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Athena / Glue
# -----------------------------------------------------------------------------

variable "create_athena_resources" {
  description = "Create Athena workgroup and Glue database"
  type        = bool
  default     = true
}

variable "glue_database_name" {
  description = "Name of the Glue database for Athena queries"
  type        = string
  default     = "schemahub"
}

# -----------------------------------------------------------------------------
# Monitoring
# -----------------------------------------------------------------------------

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "alarm_email" {
  description = "Email address for CloudWatch alarm notifications (optional)"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Billing Alerts
# -----------------------------------------------------------------------------

variable "billing_alert_email" {
  description = "Email address for billing alert notifications"
  type        = string
  default     = ""
}

variable "billing_alert_phone" {
  description = "Phone number for SMS billing alerts (E.164 format, e.g., +18476130632)"
  type        = string
  default     = ""
}

variable "billing_alert_threshold_increment" {
  description = "Dollar increment for billing alerts (e.g., 25 creates alerts at $25, $50, $75...)"
  type        = number
  default     = 25
}

variable "billing_alert_max_threshold" {
  description = "Maximum threshold for billing alerts"
  type        = number
  default     = 500
}
