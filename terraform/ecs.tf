# -----------------------------------------------------------------------------
# ECS Cluster and Task Definitions
# -----------------------------------------------------------------------------

# Get default VPC if none provided
data "aws_vpc" "default" {
  count   = var.vpc_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

locals {
  vpc_id     = var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id
  subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.default[0].ids
}

# -----------------------------------------------------------------------------
# ECS Cluster
# -----------------------------------------------------------------------------

resource "aws_ecs_cluster" "schemahub" {
  name = var.project_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "${var.project_name}-cluster"
  }
}

resource "aws_ecs_cluster_capacity_providers" "schemahub" {
  cluster_name = aws_ecs_cluster.schemahub.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE_SPOT"
  }
}

# -----------------------------------------------------------------------------
# Security Group for ECS Tasks
# -----------------------------------------------------------------------------

resource "aws_security_group" "ecs_tasks" {
  name        = "${var.project_name}-ecs-tasks"
  description = "Security group for SchemaHub ECS tasks"
  vpc_id      = local.vpc_id

  # Outbound: Allow all (needed for API calls, S3, etc.)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic"
  }

  # No inbound rules needed - tasks only make outbound calls

  tags = {
    Name = "${var.project_name}-ecs-tasks-sg"
  }
}

# -----------------------------------------------------------------------------
# Task Definition: Ingest
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "ingest" {
  family                   = "${var.project_name}-ingest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.ecs_cpu
  memory                   = var.ecs_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "ingest"
      image     = "${aws_ecr_repository.schemahub.repository_url}:latest"
      essential = true

      command = ["ingest", "--s3-bucket", var.s3_bucket_name, "--checkpoint-s3"]

      environment = [
        {
          name  = "S3_BUCKET"
          value = var.s3_bucket_name
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "DYNAMODB_LOCKS_TABLE"
          value = aws_dynamodb_table.locks.name
        }
      ]

      secrets = var.coinbase_api_key_arn != "" ? [
        {
          name      = "COINBASE_API_KEY"
          valueFrom = var.coinbase_api_key_arn
        },
        {
          name      = "COINBASE_API_SECRET"
          valueFrom = var.coinbase_api_secret_arn
        }
      ] : []

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.schemahub.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ingest"
        }
      }

      # Health check (optional - container exits after completion)
      # healthCheck = {
      #   command     = ["CMD-SHELL", "exit 0"]
      #   interval    = 30
      #   timeout     = 5
      #   retries     = 3
      #   startPeriod = 10
      # }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = {
    Name = "${var.project_name}-ingest-task"
  }
}

# -----------------------------------------------------------------------------
# Task Definition: Transform
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "transform" {
  family                   = "${var.project_name}-transform"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.ecs_task_cpu_transform
  memory                   = var.ecs_task_memory_transform
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "transform"
      image     = "${aws_ecr_repository.schemahub.repository_url}:latest"
      essential = true

      command = ["transform", "--s3-bucket", var.s3_bucket_name]

      environment = [
        {
          name  = "S3_BUCKET"
          value = var.s3_bucket_name
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "DYNAMODB_LOCKS_TABLE"
          value = aws_dynamodb_table.locks.name
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.schemahub.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "transform"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = {
    Name = "${var.project_name}-transform-task"
  }
}

# -----------------------------------------------------------------------------
# Task Definition: Full Refresh (Backfill)
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "full_refresh" {
  family                   = "${var.project_name}-full-refresh"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048 # More CPU for parallel workers
  memory                   = 4096 # More memory for large batches
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "full-refresh"
      image     = "${aws_ecr_repository.schemahub.repository_url}:latest"
      essential = true

      # Default command - override with runTask parameters for specific products
      command = [
        "ingest",
        "--full-refresh",
        "--s3-bucket", var.s3_bucket_name,
        "--checkpoint-s3",
        "--workers", "2",
        "--chunk-concurrency", "25"
      ]

      environment = [
        {
          name  = "S3_BUCKET"
          value = var.s3_bucket_name
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "DYNAMODB_LOCKS_TABLE"
          value = aws_dynamodb_table.locks.name
        }
      ]

      secrets = var.coinbase_api_key_arn != "" ? [
        {
          name      = "COINBASE_API_KEY"
          valueFrom = var.coinbase_api_key_arn
        },
        {
          name      = "COINBASE_API_SECRET"
          valueFrom = var.coinbase_api_secret_arn
        }
      ] : []

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.schemahub.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "full-refresh"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = {
    Name = "${var.project_name}-full-refresh-task"
  }
}
