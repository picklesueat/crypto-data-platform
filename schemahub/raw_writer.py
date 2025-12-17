"""Utilities for writing raw records to S3."""
from __future__ import annotations

import json
import os
import logging
from datetime import datetime
from typing import Iterable, Mapping

import boto3
from botocore.client import BaseClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def _default_serializer(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_jsonl_s3(records: Iterable[Mapping], bucket: str, key: str, s3_client: BaseClient | None = None) -> None:
    """Write records to an S3 object in JSON Lines format."""
    
    logger.debug(f"Preparing to write records to s3://{bucket}/{key}")

    client = s3_client or boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    
    payload = "".join(json.dumps(record, default=_default_serializer) + "\n" for record in records)
    payload_size = len(payload.encode("utf-8"))
    logger.debug(f"Payload size: {payload_size} bytes")
    
    try:
        client.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))
        logger.info(f"Successfully wrote {payload_size} bytes to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to write to s3://{bucket}/{key}: {e}", exc_info=True)
        raise
