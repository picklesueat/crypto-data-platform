"""Checkpoint management for backfill operations."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class LockManager:
    """Distributed lock manager using DynamoDB conditional writes.
    
    Provides atomic lock acquisition with TTL-based auto-release for crash recovery.
    Uses conditional writes to ensure only one holder can acquire a lock.
    Automatically renews locks in a background thread to prevent expiration during long jobs.
    
    Lock scopes:
        - "ingest": Covers both incremental ingest and full_refresh backfill
        - "transform": Covers transformation jobs
    
    Attributes:
        table_name: DynamoDB table name for locks
        ttl_seconds: Time-to-live for locks (auto-release on crash)
        lock_id: Unique identifier for this lock holder instance
        renewal_interval: How often to renew locks (default: ttl/2)
    """

    def __init__(self, table_name: str, ttl_seconds: int = 21600):
        """Initialize lock manager.
        
        Args:
            table_name: DynamoDB table name
            ttl_seconds: Lock TTL in seconds (default 6 hours)
        """
        self.table_name = table_name
        self.ttl_seconds = ttl_seconds
        self.lock_id = str(uuid.uuid4())
        self.dynamodb = boto3.client("dynamodb")
        self._held_locks: set[str] = set()
        self._renewal_threads: dict[str, threading.Thread] = {}
        self._stop_events: dict[str, threading.Event] = {}
        self.renewal_interval = ttl_seconds // 2  # Renew at half TTL (3 hours for 6h TTL)

    def acquire(self, lock_name: str, wait: bool = False, timeout: int = 60) -> bool:
        """Attempt to acquire a named lock.
        
        Args:
            lock_name: Name of the lock (e.g., "ingest", "transform")
            wait: If True, retry until acquired or timeout
            timeout: Max seconds to wait if wait=True
            
        Returns:
            True if lock acquired, False otherwise
        """
        ttl_epoch = int(time.time()) + self.ttl_seconds
        start_time = time.time()
        
        while True:
            try:
                self.dynamodb.put_item(
                    TableName=self.table_name,
                    Item={
                        "lock_name": {"S": lock_name},
                        "lock_id": {"S": self.lock_id},
                        "acquired_at": {"S": datetime.utcnow().isoformat() + "Z"},
                        "ttl": {"N": str(ttl_epoch)},
                    },
                    ConditionExpression="attribute_not_exists(lock_name)",
                )
                self._held_locks.add(lock_name)
                self._start_renewal_thread(lock_name)
                logger.info(f"Acquired lock '{lock_name}' with id {self.lock_id}")
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    # Lock exists, check if expired
                    if self._try_steal_expired_lock(lock_name, ttl_epoch):
                        self._held_locks.add(lock_name)
                        self._start_renewal_thread(lock_name)
                        logger.info(f"Acquired expired lock '{lock_name}' with id {self.lock_id}")
                        return True
                    
                    if not wait:
                        logger.warning(f"Lock '{lock_name}' is held by another process")
                        return False
                    
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        logger.warning(f"Timeout waiting for lock '{lock_name}'")
                        return False
                    
                    logger.info(f"Lock '{lock_name}' busy, retrying in 5s...")
                    time.sleep(5)
                else:
                    raise

    def _try_steal_expired_lock(self, lock_name: str, new_ttl: int) -> bool:
        """Try to steal a lock that has expired TTL.
        
        Uses conditional write to atomically check and replace expired lock.
        """
        try:
            # Get current lock
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={"lock_name": {"S": lock_name}},
            )
            
            if "Item" not in response:
                # Lock was released between check and get
                return False
            
            item = response["Item"]
            current_ttl = int(item.get("ttl", {}).get("N", "0"))
            current_lock_id = item.get("lock_id", {}).get("S", "")
            
            # Only steal if TTL has expired
            if current_ttl >= int(time.time()):
                return False
            
            # Conditional update - only if lock_id still matches
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item={
                    "lock_name": {"S": lock_name},
                    "lock_id": {"S": self.lock_id},
                    "acquired_at": {"S": datetime.utcnow().isoformat() + "Z"},
                    "ttl": {"N": str(new_ttl)},
                },
                ConditionExpression="lock_id = :old_id",
                ExpressionAttributeValues={":old_id": {"S": current_lock_id}},
            )
            logger.info(f"Stole expired lock '{lock_name}' from {current_lock_id}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def _start_renewal_thread(self, lock_name: str) -> None:
        """Start a background thread to periodically renew the lock TTL."""
        stop_event = threading.Event()
        self._stop_events[lock_name] = stop_event
        
        def renewal_loop():
            while not stop_event.wait(timeout=self.renewal_interval):
                if stop_event.is_set():
                    break
                try:
                    self.renew(lock_name)
                except Exception as e:
                    logger.error(f"Failed to renew lock '{lock_name}': {e}")
                    # Don't break - keep trying
        
        thread = threading.Thread(target=renewal_loop, daemon=True, name=f"lock-renewal-{lock_name}")
        thread.start()
        self._renewal_threads[lock_name] = thread
        logger.info(f"Started renewal thread for lock '{lock_name}' (interval={self.renewal_interval}s)")

    def _stop_renewal_thread(self, lock_name: str) -> None:
        """Stop the background renewal thread for a lock."""
        if lock_name in self._stop_events:
            self._stop_events[lock_name].set()
            del self._stop_events[lock_name]
        
        if lock_name in self._renewal_threads:
            thread = self._renewal_threads[lock_name]
            thread.join(timeout=5)  # Wait up to 5s for thread to stop
            del self._renewal_threads[lock_name]
            logger.info(f"Stopped renewal thread for lock '{lock_name}'")

    def renew(self, lock_name: str) -> bool:
        """Renew the TTL on a held lock.
        
        Args:
            lock_name: Name of the lock to renew
            
        Returns:
            True if renewed, False if we don't hold the lock
        """
        if lock_name not in self._held_locks:
            return False
        
        new_ttl = int(time.time()) + self.ttl_seconds
        
        try:
            self.dynamodb.update_item(
                TableName=self.table_name,
                Key={"lock_name": {"S": lock_name}},
                UpdateExpression="SET #ttl = :ttl, renewed_at = :renewed",
                ConditionExpression="lock_id = :id",
                ExpressionAttributeNames={"#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":ttl": {"N": str(new_ttl)},
                    ":renewed": {"S": datetime.utcnow().isoformat() + "Z"},
                    ":id": {"S": self.lock_id},
                },
            )
            logger.info(f"Renewed lock '{lock_name}' TTL to {new_ttl}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(f"Cannot renew lock '{lock_name}' - no longer held")
                self._held_locks.discard(lock_name)
                return False
            raise

    def release(self, lock_name: str) -> bool:
        """Release a lock.
        
        Only releases if we hold it (conditional delete).
        Stops the background renewal thread.
        
        Args:
            lock_name: Name of the lock to release
            
        Returns:
            True if released, False if we didn't hold it
        """
        # Stop renewal thread first
        self._stop_renewal_thread(lock_name)
        
        if lock_name not in self._held_locks:
            logger.warning(f"Attempted to release lock '{lock_name}' not held by this instance")
            return False
        
        try:
            self.dynamodb.delete_item(
                TableName=self.table_name,
                Key={"lock_name": {"S": lock_name}},
                ConditionExpression="lock_id = :id",
                ExpressionAttributeValues={":id": {"S": self.lock_id}},
            )
            self._held_locks.discard(lock_name)
            logger.info(f"Released lock '{lock_name}'")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(f"Lock '{lock_name}' was already released or stolen")
                self._held_locks.discard(lock_name)
                return False
            raise

    def release_all(self) -> None:
        """Release all locks held by this instance."""
        for lock_name in list(self._held_locks):
            self.release(lock_name)


class CheckpointManager:
    """Manages checkpoints for ingest operations (S3 with local fallback).
    
    Both incremental ingest and full_refresh backfill share the same checkpoint,
    ensuring seamless handoff after backfill completes.
    
    Checkpoint structure:
        {
            "cursor": int,         # Next trade ID cursor (after param for oldest->newest pagination)
            "last_updated": str,   # ISO8601 timestamp
        }
    """

    def __init__(self, s3_bucket: str, s3_prefix: str, use_s3: bool = True):
        """Initialize checkpoint manager.
        
        Args:
            s3_bucket: S3 bucket for checkpoints
            s3_prefix: S3 prefix (without /checkpoints suffix)
            use_s3: Whether to store in S3 (True) or local filesystem (False)
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.use_s3 = use_s3
        self.local_dir = "state"
        if not use_s3:
            os.makedirs(self.local_dir, exist_ok=True)
        if use_s3:
            self.s3 = boto3.client("s3")

    def _s3_key(self, product_id: str) -> str:
        """Return S3 key for a product checkpoint."""
        return f"{self.s3_prefix.rstrip('/')}/checkpoints/{product_id}.json"

    def _local_path(self, product_id: str) -> str:
        """Return local path for a product checkpoint."""
        return os.path.join(self.local_dir, f"{product_id}.json")

    def load(self, product_id: str) -> dict:
        """Load checkpoint for a product (returns empty dict if not found)."""
        if self.use_s3:
            try:
                obj = self.s3.get_object(Bucket=self.s3_bucket, Key=self._s3_key(product_id))
                data = json.loads(obj["Body"].read())
                logger.info(f"Loaded checkpoint for {product_id}: {data}")
                return data
            except self.s3.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.info(f"No checkpoint found for {product_id} (NoSuchKey)")
                    return {}
                logger.error(f"S3 error loading checkpoint for {product_id}: {e}")
                raise
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
        checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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
            tmp_path = path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(checkpoint, f)
            os.replace(tmp_path, path)


__all__ = ["CheckpointManager", "LockManager"]
