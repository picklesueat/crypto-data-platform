# -----------------------------------------------------------------------------
# Terraform Outputs
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# S3
# -----------------------------------------------------------------------------

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.schemahub.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.schemahub.arn
}

output "s3_raw_path" {
  description = "S3 path for raw data"
  value       = "s3://${aws_s3_bucket.schemahub.id}/schemahub/raw_coinbase_trades/"
}

output "s3_curated_path" {
  description = "S3 path for curated data"
  value       = "s3://${aws_s3_bucket.schemahub.id}/schemahub/curated/"
}

# -----------------------------------------------------------------------------
# ECR
# -----------------------------------------------------------------------------

output "ecr_repository_url" {
  description = "ECR repository URL for Docker images"
  value       = aws_ecr_repository.schemahub.repository_url
}

output "ecr_repository_arn" {
  description = "ECR repository ARN"
  value       = aws_ecr_repository.schemahub.arn
}

# -----------------------------------------------------------------------------
# ECS
# -----------------------------------------------------------------------------

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.schemahub.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.schemahub.arn
}

output "ecs_task_definition_ingest" {
  description = "ARN of the ingest task definition"
  value       = aws_ecs_task_definition.ingest.arn
}

output "ecs_task_definition_transform" {
  description = "ARN of the transform task definition"
  value       = aws_ecs_task_definition.transform.arn
}

output "ecs_task_definition_full_refresh" {
  description = "ARN of the full-refresh task definition"
  value       = aws_ecs_task_definition.full_refresh.arn
}

output "ecs_security_group_id" {
  description = "Security group ID for ECS tasks"
  value       = aws_security_group.ecs_tasks.id
}

output "ecs_subnet_ids" {
  description = "Subnet IDs used by ECS tasks"
  value       = local.subnet_ids
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

output "ecs_task_role_arn" {
  description = "ARN of the ECS task role"
  value       = aws_iam_role.ecs_task_role.arn
}

output "ecs_execution_role_arn" {
  description = "ARN of the ECS execution role"
  value       = aws_iam_role.ecs_execution_role.arn
}

# -----------------------------------------------------------------------------
# CloudWatch
# -----------------------------------------------------------------------------

output "cloudwatch_log_group" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.schemahub.name
}

# -----------------------------------------------------------------------------
# Athena / Glue
# -----------------------------------------------------------------------------

output "glue_database_name" {
  description = "Glue database name for Athena queries"
  value       = var.create_athena_resources ? aws_glue_catalog_database.schemahub[0].name : null
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = var.create_athena_resources ? aws_athena_workgroup.schemahub[0].name : null
}

# -----------------------------------------------------------------------------
# Useful Commands
# -----------------------------------------------------------------------------

output "docker_push_commands" {
  description = "Commands to build and push Docker image"
  value       = <<-EOT
    # Authenticate Docker to ECR
    aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${aws_ecr_repository.schemahub.repository_url}
    
    # Build and push
    docker build -t ${var.project_name} .
    docker tag ${var.project_name}:latest ${aws_ecr_repository.schemahub.repository_url}:latest
    docker push ${aws_ecr_repository.schemahub.repository_url}:latest
  EOT
}

output "run_ingest_command" {
  description = "AWS CLI command to manually run ingest task"
  value       = <<-EOT
    aws ecs run-task \
      --cluster ${aws_ecs_cluster.schemahub.name} \
      --task-definition ${aws_ecs_task_definition.ingest.family} \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${join(",", local.subnet_ids)}],securityGroups=[${aws_security_group.ecs_tasks.id}],assignPublicIp=${var.assign_public_ip ? "ENABLED" : "DISABLED"}}"
  EOT
}

output "run_transform_command" {
  description = "AWS CLI command to manually run transform task"
  value       = <<-EOT
    aws ecs run-task \
      --cluster ${aws_ecs_cluster.schemahub.name} \
      --task-definition ${aws_ecs_task_definition.transform.family} \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${join(",", local.subnet_ids)}],securityGroups=[${aws_security_group.ecs_tasks.id}],assignPublicIp=${var.assign_public_ip ? "ENABLED" : "DISABLED"}}"
  EOT
}

output "run_full_refresh_command" {
  description = "AWS CLI command to manually run full refresh (backfill)"
  value       = <<-EOT
    aws ecs run-task \
      --cluster ${aws_ecs_cluster.schemahub.name} \
      --task-definition ${aws_ecs_task_definition.full_refresh.family} \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${join(",", local.subnet_ids)}],securityGroups=[${aws_security_group.ecs_tasks.id}],assignPublicIp=${var.assign_public_ip ? "ENABLED" : "DISABLED"}}"
  EOT
}

# -----------------------------------------------------------------------------
# Billing Alerts
# -----------------------------------------------------------------------------

output "billing_alert_thresholds" {
  description = "List of billing alert thresholds configured"
  value       = local.create_billing_alerts ? local.billing_thresholds : []
}

output "billing_alerts_sns_topic_arn" {
  description = "SNS topic ARN for billing alerts"
  value       = local.create_billing_alerts ? aws_sns_topic.billing_alerts[0].arn : null
}
