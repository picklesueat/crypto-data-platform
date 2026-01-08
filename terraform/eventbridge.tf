# -----------------------------------------------------------------------------
# EventBridge Scheduler for Automated Execution
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Ingest Schedule (every 45 minutes)
# -----------------------------------------------------------------------------

resource "aws_scheduler_schedule" "ingest" {
  count = var.enable_scheduling ? 1 : 0

  name        = "${var.project_name}-ingest-schedule"
  description = "Runs SchemaHub ingest every 45 minutes"
  group_name  = "default"

  schedule_expression          = var.ingest_schedule
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.schemahub.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.ingest.arn
      task_count          = 1
      launch_type         = "FARGATE"

      network_configuration {
        subnets          = local.subnet_ids
        security_groups  = [aws_security_group.ecs_tasks.id]
        assign_public_ip = var.assign_public_ip
      }

      capacity_provider_strategy {
        capacity_provider = "FARGATE_SPOT"
        weight            = 100
        base              = 1
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = 300
      maximum_retry_attempts       = 2
    }
  }

  state = "ENABLED"
}

# -----------------------------------------------------------------------------
# Transform Schedule (every 60 minutes, offset from ingest)
# -----------------------------------------------------------------------------

resource "aws_scheduler_schedule" "transform" {
  count = var.enable_scheduling ? 1 : 0

  name        = "${var.project_name}-transform-schedule"
  description = "Runs SchemaHub transform every hour"
  group_name  = "default"

  schedule_expression          = var.transform_schedule
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  target {
    arn      = aws_ecs_cluster.schemahub.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.transform.arn
      task_count          = 1
      launch_type         = "FARGATE"

      network_configuration {
        subnets          = local.subnet_ids
        security_groups  = [aws_security_group.ecs_tasks.id]
        assign_public_ip = var.assign_public_ip
      }

      capacity_provider_strategy {
        capacity_provider = "FARGATE_SPOT"
        weight            = 100
        base              = 1
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = 600
      maximum_retry_attempts       = 2
    }
  }

  state = "ENABLED"
}
