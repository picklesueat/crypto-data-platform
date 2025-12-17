"""Checkpoint management for backfill operations."""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

import boto3


class CheckpointManager:
    """Manages checkpoints for backfill and ingest operations (S3 with local fallback).
    
    Supports separate checkpoint paths for ingest vs backfill to prevent convergence issues.
    """

    def __init__(self, s3_bucket: str, s3_prefix: str, use_s3: bool = True, mode: str = "ingest"):
        """Initialize checkpoint manager.
        
        Args:
            s3_bucket: S3 bucket for checkpoints
            s3_prefix: S3 prefix (without /checkpoints suffix)
            use_s3: Whether to store in S3 (True) or local filesystem (False)
            mode: "ingest" or "backfill" - determines checkpoint path
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.use_s3 = use_s3
        self.mode = mode
        self.local_dir = "state"
        if not use_s3:
            os.makedirs(self.local_dir, exist_ok=True)
        if use_s3:
            self.s3 = boto3.client("s3")

    def _s3_key(self, product_id: str) -> str:
        """Return S3 key for a product checkpoint, separated by mode."""
        return f"{self.s3_prefix.rstrip('/')}/checkpoints/{self.mode}/{product_id}.json"

    def _local_path(self, product_id: str) -> str:
        """Return local path for a product checkpoint, separated by mode."""
        mode_dir = os.path.join(self.local_dir, self.mode)
        return os.path.join(mode_dir, f"{product_id}.json")

    def load(self, product_id: str) -> dict:
        """Load checkpoint for a product (returns empty dict if not found)."""
        if self.use_s3:
            try:
                obj = self.s3.get_object(Bucket=self.s3_bucket, Key=self._s3_key(product_id))
                data = json.loads(obj["Body"].read())
                return data
            except self.s3.exceptions.NoSuchKey:
                return {}
            except Exception:
                return {}
        else:
            path = self._local_path(product_id)
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        return json.load(f)
                except Exception:
                    return {}
            return {}

    def save(self, product_id: str, checkpoint: dict) -> None:
        """Save checkpoint for a product."""
        checkpoint["last_updated"] = datetime.utcnow().isoformat() + "Z"
        if self.use_s3:
            key = self._s3_key(product_id)
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(checkpoint),
                ContentType="application/json",
            )
        else:
            path = self._local_path(product_id)
            # Ensure mode directory exists
            mode_dir = os.path.dirname(path)
            os.makedirs(mode_dir, exist_ok=True)
            tmp_path = path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(checkpoint, f)
            os.replace(tmp_path, path)


__all__ = ["CheckpointManager"]
