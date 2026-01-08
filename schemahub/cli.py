"""Simple CLI for Coinbase ingestion."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable
import re
import sys
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
import uuid
import json

from dotenv import load_dotenv

from schemahub.connectors.coinbase import CoinbaseConnector
from schemahub.raw_writer import write_jsonl_s3
from schemahub.checkpoint import CheckpointManager
from schemahub.transform import transform_raw_to_unified
from schemahub.validation import validate_batch_and_check_manifest, validate_full_dataset_daily
from schemahub.manifest import load_manifest, update_manifest_after_transform

# Load .env file if it exists
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


def ingest_coinbase(
    product_id: str,
    limit: int,
    bucket: str,
    prefix: str,
    cursor: int,
    target_trade_id: int,
    run_id: str,
    checkpoint_mgr: CheckpointManager | None = None,
    cache_batch_size: int = 100_000,
) -> dict:
    """Ingest trades from oldest to newest using monotonic trade ID pagination.
    
    Paginates forward from cursor using 'after' parameter with trade ID arithmetic:
    - after=1000 returns trades 1-999 (trades older than ID 1000)
    - after=2000 returns trades 1000-1999
    - etc.
    
    Args:
        product_id: Coinbase product ID
        limit: Trades per API request (max 1000)
        bucket: S3 bucket for raw trades
        prefix: S3 key prefix
        cursor: Starting cursor (trade ID to paginate from, e.g., 1000 for trades 1-999)
        target_trade_id: Stop when cursor exceeds this (the "finish line")
        run_id: Unique identifier for this run
        checkpoint_mgr: Optional checkpoint manager for saving progress
        cache_batch_size: Number of trades to cache before writing to S3 (default 100K)
    
    Returns:
        Dict with keys: records_written, final_cursor, checkpoint_ts
    """
    connector = CoinbaseConnector()
    ingest_ts = datetime.now(timezone.utc)
    
    logger.info(f"Starting ingest for {product_id}: cursor={cursor}, target={target_trade_id}, limit={limit}")
    logger.info(f"Ingest time (UTC): {ingest_ts.isoformat()}")
    
    total_records = 0
    cached_trades = []
    cached_records = []
    current_cursor = cursor
    
    while current_cursor <= target_trade_id:
        logger.info(f"[{product_id}] Fetching trades: after={current_cursor}, limit={limit}")
        
        try:
            # Fetch trades older than current_cursor (returns trades current_cursor-limit to current_cursor-1)
            trades, _ = connector.fetch_trades_with_cursor(
                product_id=product_id,
                limit=limit,
                after=current_cursor,
            )
            logger.info(f"[{product_id}] Got {len(trades)} trades from API")
        except Exception as e:
            logger.error(f"[{product_id}] API request failed: {e}", exc_info=True)
            raise
        
        if not trades:
            logger.info(f"[{product_id}] No more trades (empty response)")
            break
        
        # Cache trades and raw records
        cached_trades.extend(trades)
        cached_records.extend([connector.to_raw_record(t, product_id, ingest_ts) for t in trades])
        
        # Move cursor forward by limit (monotonic trade IDs)
        current_cursor += limit
        
        # Write to S3 and checkpoint when cache reaches threshold
        if len(cached_trades) >= cache_batch_size:
            first_trade_id = cached_trades[0].trade_id
            last_trade_id = cached_trades[-1].trade_id
            key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{product_id}_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}_{first_trade_id}_{last_trade_id}_{len(cached_trades)}.jsonl"
            
            logger.info(f"[{product_id}] Writing batch: {len(cached_trades)} trades to s3://{bucket}/{key}")
            write_jsonl_s3(cached_records, bucket=bucket, key=key)
            total_records += len(cached_trades)
            
            # Save checkpoint
            if checkpoint_mgr:
                checkpoint_mgr.save(product_id, {"cursor": current_cursor})
                logger.info(f"[{product_id}] Checkpoint saved: cursor={current_cursor}")
            
            print(f"  {product_id}: wrote {len(cached_trades)} trades (cursor={current_cursor}, target={target_trade_id})")
            cached_trades = []
            cached_records = []
    
    # Write remaining trades
    if cached_trades:
        first_trade_id = cached_trades[0].trade_id
        last_trade_id = cached_trades[-1].trade_id
        key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{product_id}_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}_{first_trade_id}_{last_trade_id}_{len(cached_trades)}.jsonl"
        
        logger.info(f"[{product_id}] Writing final batch: {len(cached_trades)} trades to s3://{bucket}/{key}")
        write_jsonl_s3(cached_records, bucket=bucket, key=key)
        total_records += len(cached_trades)
        
        # Save checkpoint
        if checkpoint_mgr:
            checkpoint_mgr.save(product_id, {"cursor": current_cursor})
            logger.info(f"[{product_id}] Checkpoint saved: cursor={current_cursor}")
        
        print(f"  {product_id}: wrote {len(cached_trades)} trades (cursor={current_cursor}, target={target_trade_id})")
    
    checkpoint_ts = datetime.now(timezone.utc).isoformat() + "Z"
    logger.info(f"[{product_id}] Ingest complete: {total_records} total records, final_cursor={current_cursor}")
    
    return {
        "records_written": total_records,
        "final_cursor": current_cursor,
        "checkpoint_ts": checkpoint_ts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SchemaHub CLI (Coinbase-only MVP)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest trades from Coinbase (oldest to newest)")
    ingest_parser.add_argument("product", nargs="?", help="Coinbase product id, e.g. BTC-USD. Required if --full-refresh is set.")
    ingest_parser.add_argument("--seed-path", default=None, help="Optional seed file path (default config/mappings/product_ids_seed.yaml)")
    ingest_parser.add_argument("--limit", type=int, default=1000, help="Number of trades per API request (default 1000)")
    ingest_parser.add_argument("--s3-bucket", default=None, help="S3 bucket for raw Coinbase trades (can also set S3_BUCKET env var)")
    ingest_parser.add_argument(
        "--s3-prefix",
        default="schemahub/raw_coinbase_trades",
        help="S3 key prefix for raw Coinbase trades",
    )
    ingest_parser.add_argument("--full-refresh", action="store_true", help="Full refresh: reset checkpoint and start from trade 1. Requires a product argument.")
    ingest_parser.add_argument("--checkpoint-s3", action="store_true", help="Store checkpoints in S3 (default: local state/ dir)")
    ingest_parser.add_argument("--workers", type=int, default=1, help="Number of concurrent product workers (default 1)")
    ingest_parser.add_argument("--dry-run", action="store_true", help="Show what would be ingested, do not fetch")

    # Simple update-seed command (barebones)
    upd = subparsers.add_parser("update-seed", help="Fetch product ids from Coinbase and update seed file")
    upd.add_argument("--path", default=None, help="Path to seed YAML (default config/mappings/product_ids_seed.yaml)")
    upd.add_argument("--merge", action="store_true", help="Merge fetched ids with existing seed file instead of replacing")
    upd.add_argument("--filter-regex", default=None, help="Only keep product ids matching this regex, e.g. '.*-USD'")
    upd.add_argument("--dry-run", action="store_true", help="Print what would be written but do not write file")

    # Transform command
    transform = subparsers.add_parser("transform", help="Transform raw JSONL to unified Parquet")
    transform.add_argument("--s3-bucket", default=None, help="S3 bucket (can also set S3_BUCKET env var)")
    transform.add_argument("--raw-prefix", default="schemahub/raw_coinbase_trades", help="S3 prefix for raw JSONL files")
    transform.add_argument("--unified-prefix", default="schemahub/unified_trades", help="S3 prefix for unified Parquet output")
    transform.add_argument("--mapping-path", default=None, help="Path to mapping YAML (optional)")
    transform.add_argument("--rebuild", action="store_true", help="Retransform all raw data to new version (safety flag)")
    transform.add_argument("--full-scan", action="store_true", help="Run full dataset validation after transform")

    return parser


def get_s3_bucket(args) -> str:
    """Get S3 bucket from CLI args or .env file. CLI takes precedence."""
    bucket = args.s3_bucket or os.getenv("S3_BUCKET")
    
    # Always log what we found for debugging
    logger.info(f"S3 Bucket Resolution: args.s3_bucket={args.s3_bucket}, env.S3_BUCKET={os.getenv('S3_BUCKET')}")
    
    if not bucket:
        error_msg = "Error: S3 bucket not specified. Provide --s3-bucket or set S3_BUCKET environment variable."
        logger.error(error_msg)
        logger.error(f"Debug: args.s3_bucket = {args.s3_bucket}")
        logger.error(f"Debug: os.getenv('S3_BUCKET') = {os.getenv('S3_BUCKET')}")
        logger.error(f"Debug: All env vars with S3: {[k for k in os.environ.keys() if 'S3' in k]}")
        logger.error(f"Debug: All env vars: {list(os.environ.keys())}")
        print(error_msg, file=sys.stderr)
        sys.exit(2)
    
    logger.info(f"Using S3 bucket: {bucket}")
    return bucket


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        s3_bucket = get_s3_bucket(args)
        connector = CoinbaseConnector()
        
        # Validate --full-refresh requires a product
        if args.full_refresh and not args.product:
            logger.error("--full-refresh requires a product argument")
            print("Error: --full-refresh requires a product argument (e.g., BTC-USD)", file=sys.stderr)
            sys.exit(2)
        
        # Determine products to run
        products_to_run = []
        if args.product:
            products_to_run = [args.product]
        else:
            # load from seed file
            products, _meta = connector.load_product_seed(path=args.seed_path)
            products_to_run = products
            logger.info(f"Loaded {len(products_to_run)} products from seed file")

        if not products_to_run:
            logger.error("No products to ingest. Provide a product or a seed file.")
            print("No products to ingest. Provide a product or a seed file.", file=sys.stderr)
            sys.exit(2)

        logger.info(f"Starting ingest for {len(products_to_run)} products (full_refresh={args.full_refresh})")
        
        # Generate unique run_id for this ingest session
        run_id = str(uuid.uuid4())
        logger.info(f"Run ID: {run_id}")
        
        # Checkpoint manager - separate paths for incremental vs full_refresh
        checkpoint_mode = "full_refresh" if args.full_refresh else "ingest"
        checkpoint_mgr = CheckpointManager(
            s3_bucket=s3_bucket,
            s3_prefix=args.s3_prefix,
            use_s3=args.checkpoint_s3,
            mode=checkpoint_mode,
        )

        total_records = 0
        run_status = "success"
        
        def process_product(pid: str) -> dict:
            """Process a single product (oldest→newest ingestion)."""
            nonlocal total_records
            
            logger.info(f"Processing product: {pid}")
            
            # Get the latest trade ID (finish line)
            try:
                target_trade_id = connector.get_latest_trade_id(pid)
                logger.info(f"[{pid}] Latest trade_id (target): {target_trade_id}")
            except Exception as e:
                logger.error(f"[{pid}] Failed to get latest trade_id: {e}")
                return {"product": pid, "status": "error", "error": str(e)}
            
            # Load checkpoint to get cursor (or start from 1000 for full refresh / cold start)
            if args.full_refresh:
                cursor = 1000  # Start from beginning
                logger.info(f"[{pid}] Full refresh: starting from cursor=1000")
            else:
                ckpt = checkpoint_mgr.load(pid)
                cursor = ckpt.get("cursor", 1000)  # Default to 1000 if no checkpoint (cold start)
                if cursor == 1000 and not ckpt:
                    logger.info(f"[{pid}] No checkpoint found, cold start from cursor=1000")
                else:
                    logger.info(f"[{pid}] Resuming from checkpoint: cursor={cursor}")
            
            # Check if already caught up
            if cursor > target_trade_id:
                logger.info(f"[{pid}] Already caught up (cursor={cursor} > target={target_trade_id})")
                print(f"  {pid}: already caught up (cursor={cursor})")
                return {"product": pid, "status": "ok", "records_written": 0, "message": "already_caught_up"}
            
            if args.dry_run:
                trades_remaining = target_trade_id - cursor
                logger.info(f"[DRY-RUN] {pid}: would ingest ~{trades_remaining} trades (cursor={cursor} to target={target_trade_id})")
                print(f"[DRY-RUN] {pid}: would ingest ~{trades_remaining} trades (cursor={cursor} to target={target_trade_id})")
                return {"product": pid, "status": "dry-run", "trades_remaining": trades_remaining}
            
            try:
                result = ingest_coinbase(
                    product_id=pid,
                    limit=args.limit,
                    bucket=s3_bucket,
                    prefix=args.s3_prefix,
                    cursor=cursor,
                    target_trade_id=target_trade_id,
                    run_id=run_id,
                    checkpoint_mgr=checkpoint_mgr,
                )
                
                records_written = result["records_written"]
                total_records += records_written
                
                logger.info(f"[{pid}] Ingest complete: {records_written} records written")
                return {"product": pid, "status": "ok", "records_written": records_written}
                
            except Exception as e:
                logger.error(f"Error ingesting {pid}: {e}", exc_info=True)
                return {"product": pid, "status": "error", "error": str(e)}
        
        # Process products (single or parallel)
        results = []
        if args.workers > 1 and len(products_to_run) > 1:
            logger.info(f"Processing {len(products_to_run)} products with {args.workers} workers")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {executor.submit(process_product, pid): pid for pid in products_to_run}
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    if result.get("status") == "error":
                        run_status = "partial_failure"
        else:
            for pid in products_to_run:
                result = process_product(pid)
                results.append(result)
                if result.get("status") == "error":
                    run_status = "partial_failure"
        
        # Print JSON summary at the end
        summary = {
            "pipeline": "coinbase_ingest",
            "status": run_status,
            "run_id": run_id,
            "records_written": total_records,
            "products_processed": len(results),
            "full_refresh": args.full_refresh,
            "checkpoint_ts": datetime.now(timezone.utc).isoformat() + "Z",
        }
        print(json.dumps(summary), flush=True)

    if args.command == "update-seed":
        logger.info("Starting update-seed command")
        connector = CoinbaseConnector()
        try:
            logger.info("Fetching products from Coinbase API")
            resp = requests.get("https://api.exchange.coinbase.com/products", timeout=10)
            resp.raise_for_status()
            products = resp.json()
            logger.info(f"Fetched {len(products)} products from Coinbase")
        except Exception as exc:  # noqa: BLE001 - keep simple
            logger.error(f"Failed to fetch products from Coinbase: {exc}", exc_info=True)
            print(f"Failed to fetch products from Coinbase: {exc}", file=sys.stderr)
            sys.exit(2)

        ids = sorted({p.get("id") for p in products if p.get("id")})
        logger.info(f"Extracted {len(ids)} product IDs")

        if args.filter_regex:
            logger.info(f"Filtering by regex: {args.filter_regex}")
            rx = re.compile(args.filter_regex)
            ids = [i for i in ids if rx.search(i)]
            logger.info(f"After filter: {len(ids)} product IDs")

        if args.merge:
            logger.info("Merging with existing seed file")
            existing, _meta = connector.load_product_seed(args.path)
            ids = sorted(set(existing) | set(ids))
            logger.info(f"After merge: {len(ids)} product IDs")

        if args.dry_run:
            logger.info(f"[DRY-RUN] Would write {len(ids)} product IDs")
            print(f"Would write {len(ids)} product ids to {args.path or 'DEFAULT'}")
            for i in ids[:200]:
                print(i)
            if len(ids) > 200:
                print("... (truncated)")
            return

        logger.info(f"Saving {len(ids)} product IDs to seed file")
        metadata = {"source": "coinbase", "count": len(ids)}
        connector.save_product_seed(ids, path=args.path, metadata=metadata)
        saved, meta = connector.load_product_seed(args.path)
        logger.info(f"Successfully wrote {len(saved)} product IDs")
        print(f"Wrote {len(saved)} product ids to {args.path or 'DEFAULT'}")

    if args.command == "transform":
        logger.info("Starting transform command")
        s3_bucket = get_s3_bucket(args)
        
        run_id = str(uuid.uuid4())
        logger.info(f"Run ID: {run_id}")
        
        # Determine output version (v1 or v2, alternating for replay safety)
        version = 1  # TODO: Read from manifest to get current version, alternate on replay
        
        result = transform_raw_to_unified(
            bucket=s3_bucket,
            raw_prefix=args.raw_prefix,
            unified_prefix=args.unified_prefix,
            mapping_path=args.mapping_path,
            version=version,
            run_id=run_id,
            rebuild=args.rebuild,
        )
        
        records_written = result.get("records_written", 0)
        s3_key = result.get("s3_key", "")
        processed_files = result.get("processed_files", [])
        status = result.get("status", "unknown")
        error = result.get("error")
        
        if s3_key:
            logger.info(f"Transform complete: wrote {records_written} records to s3://{s3_bucket}/{s3_key}")
            print(f"Transform complete: wrote {records_written} records to s3://{s3_bucket}/{s3_key}")
        else:
            logger.warning(f"Transform failed or no data: {status}")
            if error:
                logger.error(f"Error: {error}")
                print(f"Error: {error}", file=sys.stderr)
        
        # Run full dataset validation if requested
        validation_issues = []
        validation_metrics = {}
        if args.full_scan and s3_key:
            logger.info("Running full dataset validation (--full-scan)")
            try:
                validation_issues, validation_metrics = validate_full_dataset_daily(
                    bucket=s3_bucket,
                    unified_prefix=args.unified_prefix,
                )
                if validation_issues:
                    logger.warning(f"Full scan validation found {len(validation_issues)} issues:")
                    for issue in validation_issues:
                        logger.warning(f"  - {issue}")
                else:
                    logger.info("Full scan validation passed - no issues found")
            except Exception as e:
                logger.error(f"Full scan validation failed: {e}", exc_info=True)
                validation_issues = [f"Validation error: {str(e)}"]
        
        # Update manifest with processed files and validation results
        if status == "success":
            try:
                logger.info("Updating manifest with transform results")
                manifest = load_manifest(s3_bucket)
                quality_gate_passed = len(validation_issues) == 0
                manifest = update_manifest_after_transform(
                    bucket=s3_bucket,
                    manifest=manifest,
                    transform_result=result,
                    batch_issues=validation_issues,
                    batch_metrics=validation_metrics,
                    quality_gate_passed=quality_gate_passed,
                )
                logger.info("Manifest updated successfully")
            except Exception as e:
                logger.error(f"Failed to update manifest: {e}", exc_info=True)
        
        # Print JSON summary at the end
        summary = {
            "pipeline": "coinbase_transform",
            "status": status,
            "run_id": run_id,
            "records_read": result.get("records_read", 0),
            "records_transformed": result.get("records_transformed", 0),
            "records_written": records_written,
            "checkpoint_ts": datetime.now(timezone.utc).isoformat() + "Z",
            "output_version": version,
            "full_scan": args.full_scan,
            "processed_files_count": len(processed_files),
        }
        if args.full_scan:
            summary["validation_issues"] = validation_issues
            summary["validation_metrics"] = validation_metrics
        print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    # Configure logging with appropriate verbosity
    import sys
    log_level = logging.INFO
    # Check if -v or --verbose flag present
    if '-v' in sys.argv or '--verbose' in sys.argv:
        log_level = logging.DEBUG
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr),
        ]
    )
    main()
