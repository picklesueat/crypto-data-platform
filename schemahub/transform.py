"""Transform JSONL raw trades to unified Parquet format."""
from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from typing import Any
from pathlib import Path
import uuid

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

logger = logging.getLogger(__name__)


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


def read_raw_trades_from_s3(bucket: str, prefix: str) -> list[dict]:
    """Read all raw JSONL trades from S3 prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for raw files
        
    Returns:
        List of trade records (dicts)
    """
    s3 = boto3.client("s3")
    trades = []
    
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
                
                logger.info(f"Reading raw trades from s3://{bucket}/{key}")
                response = s3.get_object(Bucket=bucket, Key=key)
                body = response["Body"].read().decode("utf-8")
                
                for line in body.strip().split("\n"):
                    if not line.strip():
                        continue
                    trades.append(json.loads(line))
    
    except Exception as e:
        logger.error(f"Error reading raw trades from S3: {e}", exc_info=True)
        raise
    
    return trades


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
            "ingest_ts": datetime.now(timezone.utc).isoformat() + "Z",
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
    
    # Convert timestamp columns
    if "trade_ts" in df.columns:
        df["trade_ts"] = pd.to_datetime(df["trade_ts"])
    if "ingest_ts" in df.columns:
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"])
    
    # Create PyArrow table with proper schema
    schema = pa.schema([
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("trade_id", pa.string()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.float64()),
        ("trade_ts", pa.timestamp("us", tz="UTC")),
        ("ingest_ts", pa.timestamp("us", tz="UTC")),
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


def transform_raw_to_unified(
    bucket: str,
    raw_prefix: str,
    unified_prefix: str,
    mapping_path: str | None = None,
    version: int = 1,
    run_id: str | None = None,
) -> dict:
    """Main transform function: read raw JSONL, transform, write Parquet.
    
    Args:
        bucket: S3 bucket
        raw_prefix: S3 prefix for raw JSONL files
        unified_prefix: S3 prefix for unified Parquet output
        mapping_path: Path to mapping YAML (optional, not required for MVP)
        version: Output version (v1, v2, etc.)
        run_id: Unique run identifier
        
    Returns:
        Dict with: records_read, records_transformed, records_written, s3_key, status
    """
    logger.info(f"Starting transform: raw_prefix={raw_prefix}, unified_prefix={unified_prefix}, version={version}")
    
    try:
        # Load mapping if provided
        mapping = None
        if mapping_path:
            mapping = load_mapping(mapping_path)
            logger.info(f"Loaded mapping from {mapping_path}")
        
        # Read raw trades
        logger.info(f"Reading raw trades from s3://{bucket}/{raw_prefix}")
        raw_trades = read_raw_trades_from_s3(bucket, raw_prefix)
        logger.info(f"Read {len(raw_trades)} raw trades")
        
        if not raw_trades:
            logger.warning("No raw trades found")
            return {
                "records_read": 0,
                "records_transformed": 0,
                "records_written": 0,
                "s3_key": "",
                "status": "no_data",
                "error": "No raw trades found",
            }
        
        # Transform trades
        logger.info(f"Transforming {len(raw_trades)} raw trades to unified schema")
        unified_trades = []
        for raw_trade in raw_trades:
            unified = transform_trade(raw_trade, mapping or {})
            if unified:
                unified_trades.append(unified)
        
        logger.info(f"Transformed {len(unified_trades)} trades (skipped {len(raw_trades) - len(unified_trades)})")
        
        if not unified_trades:
            logger.warning("No trades transformed successfully")
            return {
                "records_read": len(raw_trades),
                "records_transformed": 0,
                "records_written": 0,
                "s3_key": "",
                "status": "transformation_failed",
                "error": "No trades transformed successfully",
            }
        
        # Write Parquet
        logger.info(f"Writing {len(unified_trades)} unified trades to Parquet")
        s3_key = write_unified_parquet(
            unified_trades,
            bucket,
            unified_prefix,
            version=version,
            run_id=run_id,
        )
        
        logger.info(f"Transform complete: {len(unified_trades)} records written")
        
        return {
            "records_read": len(raw_trades),
            "records_transformed": len(unified_trades),
            "records_written": len(unified_trades),
            "s3_key": s3_key,
            "status": "success",
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
