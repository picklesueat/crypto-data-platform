"""Manifest management for tracking transform state and data quality."""
from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)

MANIFEST_KEY = "schemahub/manifest.json"


def load_manifest(bucket: str, manifest_key: str = MANIFEST_KEY) -> dict:
    """Load manifest from S3, or return empty structure if not found.
    
    Args:
        bucket: S3 bucket name
        manifest_key: S3 key for manifest file
        
    Returns:
        Manifest dict with structure:
        {
            "processed_raw_files": [...],
            "product_stats": {...},
            "transform_history": [...],
            "health": {...},
            "dup_trends": [...],
            "last_version": 1,
            "last_update_ts": "...",
        }
    """
    s3 = boto3.client("s3")
    
    default_manifest = {
        "processed_raw_files": [],
        "product_stats": {},
        "transform_history": [],
        "health": {
            "last_successful_transform": None,
            "last_validation_issues": [],
            "consecutive_failures": 0,
        },
        "dup_trends": [],
        "last_version": 1,
        "last_update_ts": None,
        "replayed_versions": {},  # track v1→v2 replays for investigation
    }
    
    try:
        response = s3.get_object(Bucket=bucket, Key=manifest_key)
        manifest_bytes = response["Body"].read().decode("utf-8")
        manifest = json.loads(manifest_bytes)
        logger.info(f"Loaded manifest from s3://{bucket}/{manifest_key}")
        return manifest
    except s3.exceptions.NoSuchKey:
        logger.info(f"Manifest not found at s3://{bucket}/{manifest_key}, using default")
        return default_manifest
    except Exception as e:
        logger.warning(f"Error loading manifest: {e}, using default")
        return default_manifest


def save_manifest(bucket: str, manifest: dict, manifest_key: str = MANIFEST_KEY) -> bool:
    """Save manifest to S3.
    
    Args:
        bucket: S3 bucket name
        manifest: Manifest dict to save
        manifest_key: S3 key for manifest file
        
    Returns:
        True if successful, False otherwise
    """
    s3 = boto3.client("s3")
    
    try:
        manifest_json = json.dumps(manifest, indent=2, default=str)
        s3.put_object(Bucket=bucket, Key=manifest_key, Body=manifest_json)
        logger.info(f"Saved manifest to s3://{bucket}/{manifest_key}")
        return True
    except Exception as e:
        logger.error(f"Error saving manifest: {e}", exc_info=True)
        return False


def update_manifest_after_transform(
    bucket: str,
    manifest: dict,
    transform_result: dict,
    batch_issues: list[str],
    batch_metrics: dict,
    quality_gate_passed: bool,
    manifest_key: str = MANIFEST_KEY,
) -> dict:
    """Update manifest after a transform run and save to S3.
    
    Args:
        bucket: S3 bucket name
        manifest: Current manifest
        transform_result: Result dict from transform_raw_to_unified()
        batch_issues: Issues from batch validation
        batch_metrics: Metrics from batch validation
        quality_gate_passed: Whether quality gates passed
        manifest_key: S3 key for manifest file
        
    Returns:
        Updated manifest dict
    """
    logger.info("Updating manifest after transform")
    
    now = datetime.now(timezone.utc).isoformat()
    s3_key = transform_result.get("s3_key", "")
    
    # 1. Track processed raw files (NEW: extract from transform result)
    processed_raw_files = transform_result.get("processed_files", [])
    if processed_raw_files:
        for raw_file in processed_raw_files:
            if raw_file not in manifest["processed_raw_files"]:
                manifest["processed_raw_files"].append(raw_file)
        logger.info(f"Added {len(processed_raw_files)} new files to processed_raw_files")
    
    # 2. Update product stats
    # TODO: Extract product info from transform_result
    
    # 3. Add transform history entry
    history_entry = {
        "timestamp": now,
        "records_read": transform_result.get("records_read", 0),
        "records_transformed": transform_result.get("records_transformed", 0),
        "records_written": transform_result.get("records_written", 0),
        "status": transform_result.get("status", "unknown"),
        "output_version": transform_result.get("output_version", 1),
        "s3_key": s3_key,
        "processed_raw_files_count": len(processed_raw_files),
        "quality_gate_passed": quality_gate_passed,
        "validation_issues": batch_issues,
        "validation_metrics": batch_metrics,
    }
    manifest["transform_history"].append(history_entry)
    
    # 4. Update health
    if quality_gate_passed:
        manifest["health"]["last_successful_transform"] = now
        manifest["health"]["last_validation_issues"] = []
        manifest["health"]["consecutive_failures"] = 0
    else:
        manifest["health"]["last_validation_issues"] = batch_issues
        manifest["health"]["consecutive_failures"] += 1
        
        # Trigger replay if consecutive failures exceed threshold
        if manifest["health"]["consecutive_failures"] >= 2:
            logger.warning(f"Consecutive failures: {manifest['health']['consecutive_failures']}, marking for replay")
            manifest["_replay_triggered"] = True
    
    # 5. Track duplicate trends
    dup_entry = {
        "timestamp": now,
        "duplicates_found": batch_metrics.get("duplicates_found", 0),
        "batch_size": batch_metrics.get("batch_records_checked", 0),
    }
    manifest["dup_trends"].append(dup_entry)
    
    # Update last_update_ts
    manifest["last_update_ts"] = now
    
    # 6. Save manifest to S3
    save_manifest(bucket, manifest, manifest_key)
    
    return manifest


def should_trigger_replay(manifest: dict) -> tuple[bool, str]:
    """Check if data quality issues warrant a replay to new version.
    
    Args:
        manifest: Current manifest
        
    Returns:
        Tuple of (should_replay, reason)
    """
    # Check for explicit replay trigger
    if manifest.get("_replay_triggered"):
        return True, "Manifest replay flag set (consecutive failures)"
    
    # Check consecutive failures
    consecutive_failures = manifest.get("health", {}).get("consecutive_failures", 0)
    if consecutive_failures >= 2:
        return True, f"Consecutive failures: {consecutive_failures}"
    
    # Check duplicate ratio
    if manifest.get("dup_trends"):
        recent_dups = manifest["dup_trends"][-5:]  # Last 5 runs
        for entry in recent_dups:
            if entry.get("batch_size", 0) > 0:
                dup_ratio = entry.get("duplicates_found", 0) / entry["batch_size"]
                if dup_ratio > 0.05:  # >5% duplicates
                    return True, f"High duplicate ratio: {dup_ratio:.1%}"
    
    return False, ""


def get_next_version(manifest: dict) -> int:
    """Get the next output version for replay (alternates v1 <-> v2).
    
    Args:
        manifest: Current manifest
        
    Returns:
        Next version number (1 or 2)
    """
    current = manifest.get("last_version", 1)
    next_version = 2 if current == 1 else 1
    logger.info(f"Alternating version: {current} -> {next_version}")
    return next_version


def mark_replay(manifest: dict, old_version: int, new_version: int, reason: str) -> dict:
    """Mark a replay event in the manifest.
    
    Args:
        manifest: Current manifest
        old_version: Previous version
        new_version: New version
        reason: Reason for replay
        
    Returns:
        Updated manifest
    """
    if "replayed_versions" not in manifest:
        manifest["replayed_versions"] = {}
    
    now = datetime.now(timezone.utc).isoformat() + "Z"
    replay_key = f"{old_version}_to_{new_version}"
    
    if replay_key not in manifest["replayed_versions"]:
        manifest["replayed_versions"][replay_key] = []
    
    manifest["replayed_versions"][replay_key].append({
        "timestamp": now,
        "reason": reason,
    })
    
    logger.info(f"Replay marked: v{old_version} -> v{new_version} ({reason})")
    return manifest
