#!/usr/bin/env bash
set -euo pipefail

# SchemaHub Deploy Script
# Usage: ./deploy.sh [--skip-docker] [--skip-terraform]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SKIP_DOCKER=false
SKIP_TERRAFORM=false

for arg in "$@"; do
    case $arg in
        --skip-docker) SKIP_DOCKER=true ;;
        --skip-terraform) SKIP_TERRAFORM=true ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

AWS_REGION="us-east-1"
PROJECT_NAME="schemahub"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}"

echo "=== SchemaHub Deploy ==="
echo "AWS Account: $AWS_ACCOUNT_ID"
echo "ECR URL: $ECR_URL"
echo ""

# 1. Build and push Docker image
if [ "$SKIP_DOCKER" = false ]; then
    echo ">>> Building Docker image..."
    docker build -t "$PROJECT_NAME" .

    echo ">>> Authenticating to ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "$ECR_URL"

    echo ">>> Tagging and pushing image..."
    docker tag "$PROJECT_NAME:latest" "$ECR_URL:latest"
    docker push "$ECR_URL:latest"
    echo ">>> Docker push complete"
else
    echo ">>> Skipping Docker build/push"
fi

echo ""

# 2. Apply Terraform
if [ "$SKIP_TERRAFORM" = false ]; then
    echo ">>> Applying Terraform..."
    cd terraform
    terraform init -upgrade
    terraform apply -auto-approve
    cd ..
    echo ">>> Terraform apply complete"
else
    echo ">>> Skipping Terraform"
fi

echo ""
echo "=== Deploy complete ==="
