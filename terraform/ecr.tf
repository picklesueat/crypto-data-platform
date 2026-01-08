# -----------------------------------------------------------------------------
# ECR Repository for SchemaHub Docker Image
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "schemahub" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name = "${var.project_name}-ecr"
  }
}

# Lifecycle policy to keep last 10 images and clean up untagged
resource "aws_ecr_lifecycle_policy" "schemahub" {
  repository = aws_ecr_repository.schemahub.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Remove untagged images after 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep only last 10 tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["v", "latest"]
          countType     = "imageCountMoreThan"
          countNumber   = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Repository policy (optional - for cross-account access)
# resource "aws_ecr_repository_policy" "schemahub" {
#   repository = aws_ecr_repository.schemahub.name
#   policy     = data.aws_iam_policy_document.ecr_policy.json
# }
