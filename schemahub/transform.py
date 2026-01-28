"""Transform JSONL raw trades to unified Parquet format."""
from __future__ import annotations

import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from pathlib import Path
import uuid

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

logger = logging.getLogger(__name__)

# Parallelization constants
PARALLEL_FETCH_SIZE = 5   # Fetch 5 files concurrently (reduced from 10 for memory)
BATCH_SIZE = 500_000      # Write every 500K records (reduced from 1M for memory)


def load_mapping(mapping_path: str) -> dict:
    """Load transformation mapping from YAML file.
    
    Args:
        mapping_path: Path to mapping YAML file
        
    Returns:
        Dict with mapping configuration
    """
    import yaml
    
    with open(mapping_path) as f:
        mapping = yaml.safe_load(f)
    return mapping


def list_raw_files_from_s3(bucket: str, prefix: str) -> list[str]:
    """List all raw JSONL file keys from S3 prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for raw files
        
    Returns:
        List of S3 keys (file paths)
    """
    s3 = boto3.client("s3")
    file_keys = []
    
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if "Contents" not in page:
                continue
                
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith(".jsonl"):
                    file_keys.append(key)
    
    except Exception as e:
        logger.error(f"Error listing raw files from S3: {e}", exc_info=True)
        raise
    
    return sorted(file_keys)  # Sort for consistent ordering


def fetch_file_content(s3_client, bucket: str, key: str) -> tuple[str, list[dict]]:
    """Fetch a single file from S3 and parse JSONL.

    Args:
        s3_client: Boto3 S3 client (reused across calls)
        bucket: S3 bucket name
        key: S3 key to fetch

    Returns:
        Tuple of (key, list_of_trade_dicts)
    """
    logger.info(f"Fetching s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    trades = [json.loads(line) for line in body.strip().split("\n") if line.strip()]
    return key, trades


def iter_raw_files_parallel(s3_client, bucket: str, keys: list[str]):
    """Fetch files in parallel batches and yield results.

    Uses ThreadPoolExecutor to fetch PARALLEL_FETCH_SIZE files concurrently.

    Args:
        s3_client: Boto3 S3 client (reused)
        bucket: S3 bucket name
        keys: Pre-filtered list of S3 keys to fetch

    Yields:
        tuple[str, list[dict]]: (file_key, list_of_trade_dicts)
    """
    logger.info(f"Starting parallel fetch of {len(keys)} files ({PARALLEL_FETCH_SIZE} concurrent)")

    with ThreadPoolExecutor(max_workers=PARALLEL_FETCH_SIZE) as executor:
        # Process in batches of PARALLEL_FETCH_SIZE
        for i in range(0, len(keys), PARALLEL_FETCH_SIZE):
            batch = keys[i:i + PARALLEL_FETCH_SIZE]

            # Submit all tasks in batch AT ONCE (non-blocking)
            # All 10 threads start fetching immediately in parallel
            futures = {
                executor.submit(fetch_file_content, s3_client, bucket, k): k
                for k in batch
            }

            # Yield results as they complete (not in submission order)
            for future in as_completed(futures):
                key = futures[future]
                try:
                    yield future.result()
                except Exception as e:
                    logger.error(f"Failed to fetch {key}: {e}")


def iter_raw_files_from_s3(bucket: str, prefix: str, skip_files: list[str] | None = None):
    """Generator that yields (file_key, raw_trades_list) for each JSONL file in S3.
    
    Processes one file at a time to minimize memory usage.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for raw files
        skip_files: List of S3 keys to skip (already processed)
        
    Yields:
        Tuple of (file_key, trades_list) for each file
    """
    s3 = boto3.client("s3")
    skip_set = set(skip_files or [])
    
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if "Contents" not in page:
                continue
                
            for obj in page["Contents"]:
                key = obj["Key"]
                if not key.endswith(".jsonl"):
                    continue
                
                # Skip already processed files
                if key in skip_set:
                    logger.debug(f"Skipping already-processed file: {key}")
                    continue
                
                logger.info(f"Reading raw trades from s3://{bucket}/{key}")
                response = s3.get_object(Bucket=bucket, Key=key)
                body = response["Body"].read().decode("utf-8")
                
                # Parse trades from this file only
                file_trades = []
                for line in body.strip().split("\n"):
                    if not line.strip():
                        continue
                    file_trades.append(json.loads(line))
                
                logger.debug(f"Read {len(file_trades)} trades from {key}")
                yield key, file_trades
    
    except Exception as e:
        logger.error(f"Error reading raw trades from S3: {e}", exc_info=True)
        raise


def transform_trade(trade: dict, mapping: dict) -> dict | None:
    """Transform a single raw trade to unified schema.
    
    Args:
        trade: Raw trade record
        mapping: Mapping configuration
        
    Returns:
        Unified trade record or None if transformation fails
    """
    try:
        # Extract raw fields (Coinbase format)
        product_id = trade.get("product_id") or trade.get("product-id")
        price = float(trade.get("price", 0))
        quantity = float(trade.get("size") or trade.get("qty", 0))
        trade_id = str(trade.get("id") or trade.get("trade_id", ""))
        side = trade.get("side", "").lower()
        trade_ts_str = trade.get("time") or trade.get("timestamp")
        
        # Parse timestamp
        if isinstance(trade_ts_str, str):
            if "T" in trade_ts_str:
                # ISO format
                trade_ts = datetime.fromisoformat(trade_ts_str.replace("Z", "+00:00"))
            else:
                # Assume epoch seconds
                trade_ts = datetime.fromtimestamp(float(trade_ts_str), tz=timezone.utc)
        else:
            trade_ts = datetime.fromtimestamp(float(trade_ts_str), tz=timezone.utc)
        
        # Build unified record
        unified = {
            "exchange": "coinbase",
            "symbol": product_id,
            "trade_id": trade_id,
            "side": side,
            "price": price,
            "quantity": quantity,
            "trade_ts": trade_ts.isoformat() if trade_ts else None,
        }
        
        return unified
    
    except Exception as e:
        logger.warning(f"Failed to transform trade {trade.get('id')}: {e}")
        return None


def write_unified_parquet(
    trades: list[dict],
    bucket: str,
    prefix: str,
    version: int = 1,
    run_id: str | None = None,
) -> str:
    """Write unified trades to Parquet in S3.
    
    Deduplicates by trade_id before writing.
    
    Args:
        trades: List of unified trade dicts
        bucket: S3 bucket name
        prefix: S3 prefix for output
        version: Output version (v1, v2, etc.)
        run_id: Unique run identifier
        
    Returns:
        S3 key of written file
    """
    if not trades:
        logger.warning("No trades to write")
        return ""
    
    # Create DataFrame
    df = pd.DataFrame(trades)
    
    # Deduplicate by trade_id (keep first occurrence)
    initial_count = len(df)
    df = df.drop_duplicates(subset=["trade_id"], keep="first")
    dedup_count = initial_count - len(df)
    if dedup_count > 0:
        logger.warning(f"Deduplicated {dedup_count} duplicate trade_ids (kept {len(df)} unique trades)")
    
    # Convert timestamp columns
    if "trade_ts" in df.columns:
        df["trade_ts"] = pd.to_datetime(df["trade_ts"], format="ISO8601", utc=True, errors="coerce")
    
    # Create PyArrow table with proper schema (no ingest_ts - operational metadata belongs in raw layer only)
    schema = pa.schema([
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("trade_id", pa.string()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.float64()),
        ("trade_ts", pa.timestamp("us", tz="UTC")),
    ])
    
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Write to Parquet
    s3 = boto3.client("s3")
    
    # Create key with versioning
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    record_count = len(trades)
    key = f"{prefix.rstrip('/')}/v{version}/unified_trades_{timestamp}_{run_id}_{record_count}.parquet"
    
    logger.info(f"Writing {record_count} unified trades to s3://{bucket}/{key}")
    
    try:
        # Use BytesIO to write to S3
        import io
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
        logger.info(f"Successfully wrote unified trades to S3: {key}")
        
        return key
    
    except Exception as e:
        logger.error(f"Error writing Parquet to S3: {e}", exc_info=True)
        raise


def run_athena_dedupe(
    bucket: str,
    unified_prefix: str,
    version: int = 1,
    database: str = "schemahub",
    workgroup: str = "schemahub",
) -> dict:
    """Run Athena CTAS to deduplicate the unified trades table.
    
    This replaces all existing parquet files with a single deduplicated file.
    Uses ROW_NUMBER() to keep only one record per (exchange, symbol, trade_id).
    
    Args:
        bucket: S3 bucket name
        unified_prefix: S3 prefix for unified output (e.g., 'schemahub/unified_trades')
        version: Output version
        database: Glue database name
        workgroup: Athena workgroup name
        
    Returns:
        Dict with status, records before/after dedupe
    """
    import time
    
    athena = boto3.client("athena")
    s3 = boto3.client("s3")
    
    # Paths
    current_path = f"s3://{bucket}/{unified_prefix}/v{version}/"
    temp_path = f"s3://{bucket}/{unified_prefix}/v{version}_dedupe_temp/"
    
    logger.info(f"Starting Athena dedupe: {current_path}")
    
    try:
        # Step 1: Count records before dedupe
        count_before_query = """
        SELECT 
            COUNT(*) as total, 
            COUNT(DISTINCT CONCAT(exchange, '|', symbol, '|', trade_id)) as unique_count
        FROM curated_trades
        """
        
        count_result = _run_athena_query(athena, count_before_query, database, workgroup, bucket)
        records_before = count_result.get("total", 0)
        unique_records = count_result.get("unique_count", 0)
        
        if records_before == unique_records:
            logger.info(f"No duplicates found ({records_before} records, all unique). Skipping dedupe.")
            return {
                "status": "skipped",
                "reason": "no_duplicates",
                "records_before": records_before,
                "records_after": records_before,
            }
        
        duplicate_count = records_before - unique_records
        logger.info(f"Found {duplicate_count} duplicates ({records_before} total, {unique_records} unique). Running dedupe...")
        
        # Step 2: UNLOAD to write deduplicated data to temp location
        # Using UNLOAD instead of CTAS because workgroup enforces centralized output
        unload_query = f"""
        UNLOAD (
            SELECT exchange, symbol, trade_id, side, price, quantity, trade_ts
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY exchange, symbol, trade_id ORDER BY trade_ts DESC) as rn
                FROM curated_trades
            )
            WHERE rn = 1
        )
        TO '{temp_path}'
        WITH (format = 'PARQUET', compression = 'SNAPPY')
        """
        
        _run_athena_query(athena, unload_query, database, workgroup, bucket)
        logger.info("UNLOAD dedupe query completed")
        
        # Step 3: Delete old parquet files
        logger.info(f"Deleting old files from {current_path}")
        _delete_s3_prefix(s3, bucket, f"{unified_prefix}/v{version}/")
        
        # Step 4: Move deduped files to original location
        logger.info(f"Moving deduped files from {temp_path} to {current_path}")
        _move_s3_prefix(s3, bucket, f"{unified_prefix}/v{version}_dedupe_temp/", f"{unified_prefix}/v{version}/")
        
        logger.info(f"Dedupe complete: {records_before} -> {unique_records} records ({duplicate_count} duplicates removed)")
        
        return {
            "status": "success",
            "records_before": records_before,
            "records_after": unique_records,
            "duplicates_removed": duplicate_count,
        }
        
    except Exception as e:
        logger.error(f"Athena dedupe failed: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }


def _run_athena_query(athena, query: str, database: str, workgroup: str, bucket: str) -> dict:
    """Execute an Athena query and wait for results."""
    import time
    
    output_location = f"s3://{bucket}/athena-results/"
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    
    query_execution_id = response["QueryExecutionId"]
    logger.debug(f"Started Athena query: {query_execution_id}")
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        
        time.sleep(2)
    
    # Get results for SELECT queries
    if query.strip().upper().startswith("SELECT"):
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        rows = results.get("ResultSet", {}).get("Rows", [])
        if len(rows) > 1:
            headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
            values = [col.get("VarCharValue", "") for col in rows[1]["Data"]]
            return {h: int(v) if v.isdigit() else v for h, v in zip(headers, values)}
    
    return {}


def _delete_s3_prefix(s3, bucket: str, prefix: str):
    """Delete all objects under an S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue
        
        objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            logger.debug(f"Deleted {len(objects)} objects from s3://{bucket}/{prefix}")


def _move_s3_prefix(s3, bucket: str, src_prefix: str, dst_prefix: str):
    """Move all objects from one S3 prefix to another."""
    paginator = s3.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket, Prefix=src_prefix):
        if "Contents" not in page:
            continue
        
        for obj in page["Contents"]:
            src_key = obj["Key"]
            dst_key = src_key.replace(src_prefix, dst_prefix, 1)
            
            # Copy
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": src_key},
                Key=dst_key,
            )
            
            # Delete source
            s3.delete_object(Bucket=bucket, Key=src_key)
            
            logger.debug(f"Moved {src_key} -> {dst_key}")


def transform_raw_to_unified(
    bucket: str,
    raw_prefix: str,
    unified_prefix: str,
    mapping_path: str | None = None,
    version: int = 1,
    run_id: str | None = None,
    rebuild: bool = False,
    manifest_key: str = "schemahub/manifest.json",
) -> dict:
    """Main transform function: read raw JSONL, transform, write Parquet.
    
    Supports both full refresh (rebuild=True) and incremental (rebuild=False).
    In incremental mode, uses manifest to skip already-processed files.
    
    Args:
        bucket: S3 bucket
        raw_prefix: S3 prefix for raw JSONL files
        unified_prefix: S3 prefix for unified Parquet output
        mapping_path: Path to mapping YAML (optional, not required for MVP)
        version: Output version (v1, v2, etc.)
        run_id: Unique run identifier
        rebuild: If True, process all files (full refresh). If False, only process new files (incremental).
        manifest_key: S3 key for manifest file
        
    Returns:
        Dict with: records_read, records_transformed, records_written, s3_key, status, processed_files
    """
    logger.info(f"Starting transform: raw_prefix={raw_prefix}, unified_prefix={unified_prefix}, version={version}, rebuild={rebuild}")
    
    try:
        # Load mapping if provided
        mapping = None
        if mapping_path:
            mapping = load_mapping(mapping_path)
            logger.info(f"Loaded mapping from {mapping_path}")
        
        # Load manifest for incremental processing
        skip_files = []
        if not rebuild:
            try:
                from schemahub.manifest import load_manifest
                manifest = load_manifest(bucket, manifest_key)
                skip_files = manifest.get("processed_raw_files", [])
                logger.info(f"Incremental mode: will skip {len(skip_files)} already-processed files")
            except Exception as e:
                logger.warning(f"Could not load manifest for incremental processing: {e}, will do full refresh")
                rebuild = True
        
        if rebuild:
            logger.info("Rebuild mode: processing all raw files (full refresh)")
            skip_files = []

        # List all files and filter (separation of concerns)
        s3 = boto3.client("s3")
        all_keys = list_raw_files_from_s3(bucket, raw_prefix)
        skip_set = set(skip_files)
        keys_to_process = [k for k in all_keys if k not in skip_set]

        logger.info(f"Found {len(keys_to_process)} files to process ({len(skip_set)} skipped)")

        if not keys_to_process:
            logger.warning("No new files to process")
            return {
                "records_read": 0,
                "records_transformed": 0,
                "records_written": 0,
                "s3_key": "",
                "status": "no_new_files",
                "error": "No new files to process (all already processed)",
                "processed_files": [],
            }

        # Process with parallel fetching
        unified_trades = []
        processed_files = []
        total_raw_count = 0
        total_transformed_count = 0
        total_written_count = 0
        output_keys = []

        for file_key, file_trades in iter_raw_files_parallel(s3, bucket, keys_to_process):
            logger.info(f"Transforming {len(file_trades)} trades from {file_key}")
            total_raw_count += len(file_trades)
            
            # Transform trades from this file
            for raw_trade in file_trades:
                unified = transform_trade(raw_trade, mapping or {})
                if unified:
                    unified_trades.append(unified)
                    total_transformed_count += 1
                    
                    # Batch write when threshold reached
                    if len(unified_trades) >= BATCH_SIZE:
                        logger.info(f"Batch size reached ({len(unified_trades)} records), writing to Parquet")
                        s3_key = write_unified_parquet(
                            unified_trades,
                            bucket,
                            unified_prefix,
                            version=version,
                            run_id=run_id,
                        )
                        output_keys.append(s3_key)
                        total_written_count += len(unified_trades)
                        unified_trades = []  # Clear batch
            
            processed_files.append(file_key)
            logger.debug(f"Processed {file_key}: {total_transformed_count} total transformed, {len(unified_trades)} in current batch")
        
        # Write remaining trades
        if unified_trades:
            logger.info(f"Writing final batch of {len(unified_trades)} unified trades to Parquet")
            s3_key = write_unified_parquet(
                unified_trades,
                bucket,
                unified_prefix,
                version=version,
                run_id=run_id,
            )
            output_keys.append(s3_key)
            total_written_count += len(unified_trades)
        
        logger.info(f"Transform complete: processed {len(processed_files)} files, read {total_raw_count} raw trades, transformed {total_transformed_count} trades, wrote {total_written_count} records to {len(output_keys)} Parquet files")
        
        if total_raw_count == 0:
            logger.warning("No raw trades found")
            return {
                "records_read": 0,
                "records_transformed": 0,
                "records_written": 0,
                "s3_key": "",
                "status": "no_data",
                "error": "No raw trades found",
                "processed_files": processed_files,
            }
        
        if total_transformed_count == 0:
            logger.warning("No trades transformed successfully")
            return {
                "records_read": total_raw_count,
                "records_transformed": 0,
                "records_written": 0,
                "s3_key": "",
                "status": "transformation_failed",
                "error": "No trades transformed successfully",
                "processed_files": processed_files,
            }
        
        # Run Athena CTAS dedupe to guarantee no duplicates across all parquet files
        dedupe_result = run_athena_dedupe(
            bucket=bucket,
            unified_prefix=unified_prefix,
            version=version,
        )
        
        # Return last key for backwards compatibility (validation checks this)
        primary_key = output_keys[-1] if output_keys else ""
        
        return {
            "records_read": total_raw_count,
            "records_transformed": total_transformed_count,
            "records_written": total_written_count,
            "s3_key": primary_key,
            "output_keys": output_keys,  # All written files
            "status": "success",
            "processed_files": processed_files,
            "dedupe_result": dedupe_result,
        }
    
    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        return {
            "records_read": 0,
            "records_transformed": 0,
            "records_written": 0,
            "s3_key": "",
            "status": "error",
            "error": str(e),
        }
