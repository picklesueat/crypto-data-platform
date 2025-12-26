"""Data quality validation functions for unified trades."""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def validate_batch_and_check_manifest(
    bucket: str,
    unified_prefix: str,
    latest_s3_key: str,
    manifest_data: dict | None = None,
) -> tuple[list[str], dict]:
    """Hourly batch validation: schema, dups on batch, stale product check.
    
    Args:
        bucket: S3 bucket name
        unified_prefix: S3 prefix for unified Parquet files
        latest_s3_key: S3 key of the latest Parquet file written
        manifest_data: Current manifest data (optional)
        
    Returns:
        Tuple of (issues_list, metrics_dict)
        - issues_list: List of validation issues found (empty if all good)
        - metrics_dict: Metrics about this validation run
    """
    logger.info("Starting batch validation")
    
    issues = []
    metrics = {
        "batch_records_checked": 0,
        "duplicates_found": 0,
        "schema_errors": 0,
        "stale_products": [],
    }
    
    try:
        # Read latest Parquet file
        s3 = boto3.client("s3")
        
        if not latest_s3_key:
            logger.warning("No latest S3 key provided, skipping batch validation")
            issues.append("No latest Parquet file to validate")
            return issues, metrics
        
        logger.info(f"Validating batch from s3://{bucket}/{latest_s3_key}")
        
        # Download Parquet and convert to DataFrame
        response = s3.get_object(Bucket=bucket, Key=latest_s3_key)
        import io
        parquet_bytes = io.BytesIO(response["Body"].read())
        
        import pyarrow.parquet as pq
        table = pq.read_table(parquet_bytes)
        df = table.to_pandas()
        
        metrics["batch_records_checked"] = len(df)
        logger.info(f"Read {len(df)} records from latest Parquet")
        
        # 1. Check schema
        required_columns = {"exchange", "symbol", "trade_id", "side", "price", "quantity", "trade_ts", "ingest_ts"}
        missing_columns = required_columns - set(df.columns)
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            issues.append(error_msg)
            metrics["schema_errors"] += 1
        
        # 2. Check for duplicates within batch
        if "trade_id" in df.columns:
            duplicates = df[df.duplicated(subset=["trade_id"], keep=False)]
            if len(duplicates) > 0:
                dup_count = len(duplicates) // 2  # Each dup appears twice
                error_msg = f"Found {dup_count} duplicate trade_ids in batch"
                logger.warning(error_msg)
                issues.append(error_msg)
                metrics["duplicates_found"] = dup_count
        
        # 3. Check numeric columns
        numeric_checks = {
            "price": {"min": 0, "allow_zero": False},
            "quantity": {"min": 0, "allow_zero": False},
        }
        
        for col, checks in numeric_checks.items():
            if col in df.columns:
                invalid = df[df[col] < checks["min"]]
                if len(invalid) > 0:
                    error_msg = f"Found {len(invalid)} records with invalid {col} (negative or zero when not allowed)"
                    logger.warning(error_msg)
                    issues.append(error_msg)
        
        # 4. Check side enum
        if "side" in df.columns:
            valid_sides = {"buy", "sell"}
            invalid_sides = set(df["side"].unique()) - valid_sides
            if invalid_sides:
                error_msg = f"Found invalid side values: {invalid_sides}"
                logger.warning(error_msg)
                issues.append(error_msg)
        
        # 5. Check for stale products in manifest
        if manifest_data:
            now = datetime.now(timezone.utc)
            product_stats = manifest_data.get("product_stats", {})
            
            for symbol, stats in product_stats.items():
                last_update = stats.get("last_update_ts")
                if last_update:
                    last_update_dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                    staleness = now - last_update_dt
                    
                    if staleness > timedelta(hours=2):
                        stale_msg = f"Product {symbol} hasn't received new data in {staleness}"
                        logger.warning(stale_msg)
                        metrics["stale_products"].append(symbol)
                        # Don't add to issues yet - this is informational
        
        logger.info(f"Batch validation complete: {len(issues)} issues found")
        
    except Exception as e:
        logger.error(f"Error during batch validation: {e}", exc_info=True)
        issues.append(f"Validation error: {str(e)}")
    
    return issues, metrics


def validate_full_dataset_daily(
    bucket: str,
    unified_prefix: str,
) -> tuple[list[str], dict]:
    """Daily full dataset validation: freshness, gaps, comprehensive dups check.
    
    Args:
        bucket: S3 bucket name
        unified_prefix: S3 prefix for unified Parquet files
        
    Returns:
        Tuple of (issues_list, metrics_dict)
    """
    logger.info("Starting daily full dataset validation")
    
    issues = []
    metrics = {
        "total_records": 0,
        "duplicates_found": 0,
        "stale_records": 0,
        "date_range": {},
        "products": [],
    }
    
    try:
        s3 = boto3.client("s3")
        
        # List all Parquet files in unified prefix
        logger.info(f"Scanning s3://{bucket}/{unified_prefix} for all Parquet files")
        
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=unified_prefix)
        
        parquet_keys = []
        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    parquet_keys.append(obj["Key"])
        
        logger.info(f"Found {len(parquet_keys)} Parquet files to validate")
        
        if not parquet_keys:
            error_msg = "No Parquet files found in unified prefix"
            logger.warning(error_msg)
            issues.append(error_msg)
            return issues, metrics
        
        # Read all Parquet files and combine
        import pyarrow.parquet as pq
        import io
        
        dfs = []
        for key in parquet_keys:
            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                parquet_bytes = io.BytesIO(response["Body"].read())
                table = pq.read_table(parquet_bytes)
                dfs.append(table.to_pandas())
            except Exception as e:
                logger.warning(f"Could not read {key}: {e}")
        
        if not dfs:
            error_msg = "Could not read any Parquet files"
            logger.error(error_msg)
            issues.append(error_msg)
            return issues, metrics
        
        df = pd.concat(dfs, ignore_index=True)
        metrics["total_records"] = len(df)
        logger.info(f"Combined {len(dfs)} Parquet files into {len(df)} total records")
        
        # 1. Check freshness
        if "trade_ts" in df.columns:
            df["trade_ts"] = pd.to_datetime(df["trade_ts"])
            latest_trade = df["trade_ts"].max()
            now = datetime.now(timezone.utc)
            age = now - latest_trade.replace(tzinfo=timezone.utc)
            
            metrics["date_range"] = {
                "earliest": str(df["trade_ts"].min()),
                "latest": str(latest_trade),
                "age_hours": age.total_seconds() / 3600,
            }
            
            if age > timedelta(hours=1):
                freshness_issue = f"Latest data is {age.total_seconds() / 3600:.1f} hours old (> 1h threshold)"
                logger.warning(freshness_issue)
                issues.append(freshness_issue)
        
        # 2. Check for duplicates across all data
        if "trade_id" in df.columns:
            duplicates = df[df.duplicated(subset=["trade_id"], keep=False)]
            if len(duplicates) > 0:
                dup_count = len(duplicates) // 2
                dup_issue = f"Found {dup_count} total duplicate trade_ids across full dataset"
                logger.warning(dup_issue)
                issues.append(dup_issue)
                metrics["duplicates_found"] = dup_count
        
        # 3. Check for stale records (older than 7 days)
        if "trade_ts" in df.columns:
            cutoff = now - timedelta(days=7)
            stale = df[df["trade_ts"] < cutoff]
            metrics["stale_records"] = len(stale)
            
            if len(stale) > 0:
                stale_pct = (len(stale) / len(df)) * 100
                if stale_pct > 10:  # Flag if >10% stale
                    stale_issue = f"{stale_pct:.1f}% of records are older than 7 days"
                    logger.warning(stale_issue)
                    issues.append(stale_issue)
        
        # 4. Check products coverage
        if "symbol" in df.columns:
            products = sorted(df["symbol"].unique().tolist())
            metrics["products"] = products
            logger.info(f"Data covers {len(products)} products: {', '.join(products[:5])}...")
        
        logger.info(f"Daily validation complete: {len(issues)} issues found, {len(df)} total records")
        
    except Exception as e:
        logger.error(f"Error during daily validation: {e}", exc_info=True)
        issues.append(f"Validation error: {str(e)}")
    
    return issues, metrics


def check_data_quality_gates(
    batch_issues: list[str],
    batch_metrics: dict,
    full_issues: list[str] | None = None,
    full_metrics: dict | None = None,
) -> tuple[bool, list[str]]:
    """Check if data quality gates pass.
    
    Args:
        batch_issues: Issues from batch validation
        batch_metrics: Metrics from batch validation
        full_issues: Issues from full validation (optional, for daily runs)
        full_metrics: Metrics from full validation (optional)
        
    Returns:
        Tuple of (passes_gate, reasons_for_failure)
    """
    logger.info("Checking data quality gates")
    
    failure_reasons = []
    
    # Batch validation gates
    if batch_issues:
        # Some issues are warnings, some are failures
        for issue in batch_issues:
            if "Missing required columns" in issue or "Validation error" in issue:
                failure_reasons.append(f"BATCH_VALIDATION: {issue}")
    
    # Duplicate threshold: fail if >5% duplicates in batch
    if batch_metrics.get("duplicates_found", 0) > 0:
        dup_count = batch_metrics["duplicates_found"]
        batch_size = batch_metrics.get("batch_records_checked", 0)
        if batch_size > 0:
            dup_pct = (dup_count / batch_size) * 100
            if dup_pct > 5:
                failure_reasons.append(f"DUPLICATES: {dup_pct:.1f}% duplicates in batch (threshold: 5%)")
    
    # Full validation gates (if provided)
    if full_issues:
        for issue in full_issues:
            if "Missing required columns" in issue or "Validation error" in issue:
                failure_reasons.append(f"FULL_VALIDATION: {issue}")
    
    if full_metrics:
        # Freshness gate: fail if data >4 hours old
        age_hours = full_metrics.get("date_range", {}).get("age_hours", 0)
        if age_hours > 4:
            failure_reasons.append(f"FRESHNESS: Data is {age_hours:.1f}h old (threshold: 4h)")
    
    passes_gate = len(failure_reasons) == 0
    logger.info(f"Quality gates: {'PASS' if passes_gate else 'FAIL'}")
    if failure_reasons:
        for reason in failure_reasons:
            logger.warning(f"  - {reason}")
    
    return passes_gate, failure_reasons
