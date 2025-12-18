"""Simple CLI for Coinbase ingestion."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from typing import Iterable
import re
import sys
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
import uuid

from dotenv import load_dotenv

from schemahub.connectors.coinbase import CoinbaseConnector, _parse_time
from schemahub.raw_writer import write_jsonl_s3
from schemahub.checkpoint import CheckpointManager

# Load .env file if it exists
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


def ingest_coinbase(
    product_id: str,
    limit: int,
    bucket: str,
    prefix: str,
    after: int | None,
    run_id: str,
    time_cutoff_minutes: int = 90,
) -> tuple[str, int | None]:
    """Ingest recent trades forward from watermark, with time-based cutoff.
    
    Paginates forward from watermark using 'after' parameter until:
    1. Reaches the end of available trades, or
    2. Trades are older than time_cutoff_minutes
    
    Caches all trades in memory before writing to S3 in a single batch.
    
    Args:
        product_id: Coinbase product ID
        limit: Trades per API request
        bucket: S3 bucket for raw trades
        prefix: S3 key prefix
        after: Trade ID lower bound (watermark) - paginate forward from this
        run_id: Unique identifier for this run (ensures unique S3 keys even within same second)
        time_cutoff_minutes: Stop fetching if trades older than this (default 90 min to avoid gaps between runs)
    
    Returns:
        Tuple of (S3 key where trades were written, last_trade_id if any trades written, else None)
    """
    connector = CoinbaseConnector()
    ingest_ts = datetime.now(timezone.utc)
    cutoff_time = ingest_ts - timedelta(minutes=time_cutoff_minutes)
    
    logger.info(f"Starting ingest for {product_id}: limit={limit}, after={after}, cutoff_minutes={time_cutoff_minutes}")
    logger.info(f"Ingest time (UTC): {ingest_ts.isoformat()}")
    logger.info(f"Cutoff time (UTC): {cutoff_time.isoformat()} ({time_cutoff_minutes} min ago)")
    
    all_trades = []
    cursor = None  # Start with no cursor to get newest trades
    batch_count = 0
    
    while True:
        batch_count += 1
        logger.info(f"[{product_id}] Batch {batch_count}: Fetching trades with cursor={cursor}")
        
        try:
            # Fetch batch of trades
            # First call: cursor=None -> gets newest trades
            # Subsequent calls: cursor=next_cursor -> paginate backward
            logger.debug(f"Making API request for {product_id}: after={cursor}, limit={limit}")
            trades, next_cursor = connector.fetch_trades_with_cursor(
                product_id=product_id, 
                limit=limit, 
                after=cursor  # None on first call, then uses next_cursor for pagination backward
            )
            logger.info(f"[{product_id}] Batch {batch_count}: Got {len(trades)} trades from API, next_cursor={next_cursor}")
        except Exception as e:
            logger.error(f"[{product_id}] API request failed: {e}", exc_info=True)
            raise
        
        if not trades:
            logger.info(f"[{product_id}] No more trades fetched (empty response)")
            break
        
        # Filter trades: keep only those within time window
        batch_trades = []
        hit_cutoff = False
        logger.debug(f"[{product_id}] Batch {batch_count}: Filtering {len(trades)} trades by time window (cutoff: {cutoff_time})")
        
        for i, trade in enumerate(trades):
            trade_time = _parse_time(trade.time)
            logger.debug(f"[{product_id}] Trade {i+1}: ID={trade.trade_id}, API time={trade.time}, parsed_time={trade_time.isoformat()}, cutoff={cutoff_time.isoformat()}")
            
            # If we reach the watermark, we've caught up to the previous run
            if after is not None and trade.trade_id <= after:
                logger.info(f"[{product_id}] Batch {batch_count}: Reached watermark at trade ID {trade.trade_id} (watermark={after})")
                hit_cutoff = True
                break
            
            if trade_time < cutoff_time:
                # Trade is older than cutoff, stop fetching (everything older will be too)
                logger.info(f"[{product_id}] Batch {batch_count}: Hit time cutoff at trade {i+1}/{len(trades)}: {trade_time} < {cutoff_time}")
                hit_cutoff = True
                break
            
            batch_trades.append(trade)
        
        logger.info(f"[{product_id}] Batch {batch_count}: Kept {len(batch_trades)}/{len(trades)} trades (hit_cutoff={hit_cutoff})")
        all_trades.extend(batch_trades)
        
        # Stop if we hit cutoff
        if hit_cutoff:
            logger.info(f"[{product_id}] Stopping: hit time cutoff")
            break
        
        # Stop if no more trades or cursor didn't advance
        if not next_cursor:
            logger.info(f"[{product_id}] Stopping: no next_cursor from API")
            break
        
        logger.info(f"[{product_id}] Batch {batch_count}: Continuing with cursor={next_cursor}")
        cursor = next_cursor
    
    if not all_trades:
        # No trades within time window and watermark
        logger.info(f"No trades within time window for {product_id}")
        return "", None
    
    logger.info(f"Collected {len(all_trades)} trades for {product_id}")
    # Cache all trades locally, then write to S3 in single batch
    logger.info(f"Converting {len(all_trades)} trades to raw records")
    raw_records = [connector.to_raw_record(trade, product_id, ingest_ts) for trade in all_trades]
    logger.info(f"Created {len(raw_records)} raw records")
    
    # Write all cached records to S3
    key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}.jsonl"
    logger.info(f"Writing {len(raw_records)} records to s3://{bucket}/{key}")
    try:
        write_jsonl_s3(raw_records, bucket=bucket, key=key)
        logger.info(f"Successfully wrote trades for {product_id} to S3")
    except Exception as e:
        logger.error(f"Failed to write trades to S3 for {product_id}: {e}", exc_info=True)
        raise
    
    # Return key and the last (oldest) trade_id fetched for checkpoint
    last_trade_id = all_trades[0].trade_id if all_trades else None
    logger.info(f"Returning last_trade_id={last_trade_id} from {len(all_trades)} trades")
    return key, last_trade_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SchemaHub CLI (Coinbase-only MVP)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest from Coinbase")
    ingest_parser.add_argument("product", nargs="?", help="Coinbase product id, e.g. BTC-USD. If omitted, use seed file")
    ingest_parser.add_argument("--seed-path", default=None, help="Optional seed file path (default config/mappings/product_ids_seed.yaml)")
    ingest_parser.add_argument("--limit", type=int, default=1000, help="Number of trades to request (default 1000, max 100 per Coinbase API)")
    ingest_parser.add_argument("--before", type=int, default=None, help="Paginate using a trade_id upper bound")
    ingest_parser.add_argument("--after", type=int, default=None, help="Paginate using a trade_id lower bound")
    ingest_parser.add_argument("--s3-bucket", default=None, help="S3 bucket for raw Coinbase trades (can also set S3_BUCKET env var)")
    ingest_parser.add_argument(
        "--s3-prefix",
        default="schemahub/raw_coinbase_trades",
        help="S3 key prefix for raw Coinbase trades",
    )
    ingest_parser.add_argument("--skip-checkpoint", action="store_true", help="Skip watermark/checkpoint (force fresh fetch). Default: resume from last checkpoint.")

    # Simple update-seed command (barebones)
    upd = subparsers.add_parser("update-seed", help="Fetch product ids from Coinbase and update seed file")
    upd.add_argument("--path", default=None, help="Path to seed YAML (default config/mappings/product_ids_seed.yaml)")
    upd.add_argument("--merge", action="store_true", help="Merge fetched ids with existing seed file instead of replacing")
    upd.add_argument("--filter-regex", default=None, help="Only keep product ids matching this regex, e.g. '.*-USD'")
    upd.add_argument("--dry-run", action="store_true", help="Print what would be written but do not write file")

    # Backfill command
    backfill = subparsers.add_parser("backfill", help="Backfill trades for products from seed file")
    backfill.add_argument("--seed-path", default=None, help="Seed file path (default config/mappings/product_ids_seed.yaml)")
    backfill.add_argument("--chunk-size", type=int, default=1000, help="Number of trades per request (default 1000, max 100 per Coinbase API, batched locally)")
    backfill.add_argument("--workers", type=int, default=1, help="Number of concurrent product workers (default 1)")
    backfill.add_argument("--resume", action="store_true", help="Resume from checkpoints")
    backfill.add_argument("--s3-bucket", default=None, help="S3 bucket for raw Coinbase trades (can also set S3_BUCKET env var)")
    backfill.add_argument("--s3-prefix", default="schemahub/raw_coinbase_trades", help="S3 key prefix")
    backfill.add_argument("--checkpoint-s3", action="store_true", help="Store checkpoints in S3 (default: local state/ dir)")
    backfill.add_argument("--dry-run", action="store_true", help="Show what would be backfilled, do not ingest")

    return parser


def get_s3_bucket(args) -> str:
    """Get S3 bucket from CLI args or .env file. CLI takes precedence."""
    bucket = args.s3_bucket or os.getenv("S3_BUCKET")
    if not bucket:
        print("Error: S3 bucket not specified. Provide --s3-bucket or set S3_BUCKET environment variable.", file=sys.stderr)
        sys.exit(2)
    return bucket


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        s3_bucket = get_s3_bucket(args)
        connector = CoinbaseConnector()
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

        logger.info(f"Starting ingest for {len(products_to_run)} products")
        # Generate unique run_id for this ingest session
        run_id = str(uuid.uuid4())
        logger.info(f"Run ID: {run_id}")
        
        # Checkpoint manager is always enabled (watermark pattern)
        checkpoint_mgr = CheckpointManager(
            s3_bucket=s3_bucket,
            s3_prefix=args.s3_prefix,
            use_s3=True,
            mode="ingest",
        )

        for pid in products_to_run:
            logger.info(f"Processing product: {pid}")
            # Load watermark/checkpoint unless --skip-checkpoint is set
            after = args.after
            if not args.skip_checkpoint:
                ckpt = checkpoint_mgr.load(pid)
                if ckpt:
                    after = ckpt.get("last_trade_id")
                    logger.info(f"Resuming {pid} from trade_id {after}")
                    print(f"  Resuming {pid} from trade_id {after}")
                else:
                    logger.debug(f"No checkpoint found for {pid}, starting fresh")
            else:
                logger.debug(f"Skipping checkpoint for {pid}")

            try:
                key, last_trade_id = ingest_coinbase(
                    product_id=pid,
                    limit=args.limit,
                    bucket=s3_bucket,
                    prefix=args.s3_prefix,
                    after=after,
                    run_id=run_id,
                )
                
                if key:
                    logger.info(f"Wrote Coinbase trades for {pid} to s3://{s3_bucket}/{key}")
                    print(f"Wrote Coinbase trades for {pid} to s3://{s3_bucket}/{key}")
                else:
                    logger.info(f"No trades within time window for {pid}")
                    print(f"No trades within time window for {pid}")

                # Update watermark with last trade_id and ingest time
                ingest_ts = datetime.now(timezone.utc)
                checkpoint_data = {
                    "last_ingest_time": ingest_ts.isoformat() + "Z"
                }
                
                # Update last_trade_id only if we fetched new trades
                if last_trade_id is not None:
                    checkpoint_data["last_trade_id"] = last_trade_id
                    logger.info(f"Saving checkpoint for {pid}: last_trade_id={last_trade_id}")
                elif after is not None:
                    # Keep existing watermark if no new trades fetched this run
                    checkpoint_data["last_trade_id"] = after
                    logger.info(f"No new trades, keeping watermark: last_trade_id={after}")
                
                logger.info(f"Writing checkpoint to S3 for {pid}")
                checkpoint_mgr.save(pid, checkpoint_data)
                logger.info(f"Checkpoint saved successfully for {pid}")
            except Exception as e:
                logger.error(f"Error ingesting {pid}: {e}", exc_info=True)
                raise

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

    if args.command == "backfill":
        logger.info("Starting backfill command")
        s3_bucket = get_s3_bucket(args)
        connector = CoinbaseConnector()
        products, _meta = connector.load_product_seed(path=args.seed_path)
        logger.info(f"Loaded {len(products)} products from seed file")
        
        if not products:
            logger.error("No products in seed file")
            print("No products in seed file. Run 'update-seed' first.", file=sys.stderr)
            sys.exit(2)
        
        # Generate unique run_id for this backfill session
        run_id = str(uuid.uuid4())
        logger.info(f"Run ID: {run_id}")
        
        checkpoint_mgr = CheckpointManager(
            s3_bucket=s3_bucket,
            s3_prefix=args.s3_prefix,
            use_s3=args.checkpoint_s3,
            mode="backfill",
        ) if args.resume else None
        
        if checkpoint_mgr:
            logger.info("Checkpoint manager enabled for backfill")
        
        if args.dry_run:
            logger.info(f"[DRY-RUN] Would backfill {len(products)} products")
            print(f"[DRY-RUN] Would backfill {len(products)} products:")
            for pid in products:
                backfill_product(pid, args.chunk_size, s3_bucket, args.s3_prefix, checkpoint_mgr, run_id, dry_run=True)
            return
        
        logger.info(f"Starting backfill for {len(products)} products with {args.workers} workers")
        print(f"Starting backfill for {len(products)} products with {args.workers} workers...")
        results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    backfill_product,
                    pid,
                    args.chunk_size,
                    s3_bucket,
                    args.s3_prefix,
                    checkpoint_mgr,
                    run_id,
                    False,
                ): pid for pid in products
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                status = result.get("status", "unknown")
                trades = result.get("trades_fetched", 0)
                logger.info(f"Backfill {result['product']}: {status} ({trades} trades)")
                print(f"Backfill {result['product']}: {status} ({trades} trades)")
        
        ok_count = sum(1 for r in results if r.get("status") == "ok")
        logger.info(f"Backfill complete: {ok_count}/{len(products)} products successful")
        print(f"\nBackfill complete: {ok_count}/{len(products)} products successful")


def backfill_product(product_id: str, chunk_size: int, bucket: str, prefix: str, checkpoint_mgr: CheckpointManager, run_id: str, dry_run: bool = False) -> dict:
    """Backfill trades for a single product."""
    connector = CoinbaseConnector()
    logger.info(f"Backfilling product: {product_id}")
    
    # Load checkpoint to resume
    ckpt = checkpoint_mgr.load(product_id) if checkpoint_mgr else {}
    last_trade_id = ckpt.get("last_trade_id")
    
    if dry_run:
        logger.info(f"[DRY-RUN] Would backfill {product_id} from trade_id {last_trade_id or 'beginning'}")
        print(f"[DRY-RUN] Would backfill {product_id} from trade_id {last_trade_id or 'beginning'}")
        return {"product": product_id, "status": "dry-run"}
    
    try:
        ingest_ts = datetime.now(timezone.utc)
        after = last_trade_id
        trades_fetched = 0
        cache_batch_size = 100_00  # Cache 100K trades locally before writing to S3
        cached_records = []
        cached_trades = []
        
        while True:
            logger.debug(f"Fetching batch for {product_id}: after={after}")
            page_trades, next_after = connector.fetch_trades_with_cursor(
                product_id=product_id,
                limit=chunk_size,
                after=after,
            )
            
            if not page_trades:
                logger.debug(f"No more trades for {product_id}")
                break
            
            trades_fetched += len(page_trades)
            logger.debug(f"Fetched {len(page_trades)} trades for {product_id} (total: {trades_fetched})")
            
            # Cache trades and raw records locally
            cached_trades.extend(page_trades)
            cached_records.extend([connector.to_raw_record(t, product_id, ingest_ts) for t in page_trades])
            
            # Use the CB-AFTER header from the response for the next cursor, fallback to first trade_id
            if next_after is not None:
                new_last_trade_id = next_after
            else:
                new_last_trade_id = page_trades[0].trade_id
            
            after = new_last_trade_id
            
            # Write to S3 and checkpoint when cache reaches 1M trades
            if len(cached_trades) >= cache_batch_size:
                first_trade_id = cached_trades[0].trade_id
                last_trade_id_in_batch = cached_trades[-1].trade_id
                key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{product_id}_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}_{first_trade_id}_{last_trade_id_in_batch}_{len(cached_trades)}.jsonl"
                logger.info(f"Writing batch for {product_id}: {len(cached_trades)} trades to s3://{bucket}/{key}")
                write_jsonl_s3(cached_records, bucket=bucket, key=key)
                
                if checkpoint_mgr:
                    checkpoint_mgr.save(product_id, {"last_trade_id": new_last_trade_id, "trades_processed": trades_fetched})
                    logger.debug(f"Checkpoint saved for {product_id}: {trades_fetched} trades processed")
                
                logger.info(f"{product_id}: wrote {len(cached_trades)} trades to S3 (total: {trades_fetched})")
                print(f"  {product_id}: wrote {len(cached_trades)} trades to S3 (total: {trades_fetched})")
                cached_records = []
                cached_trades = []
        
        # Write any remaining trades
        if cached_trades:
            first_trade_id = cached_trades[0].trade_id
            last_trade_id_in_batch = cached_trades[-1].trade_id
            key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{product_id}_{ingest_ts:%Y%m%dT%H%M%SZ}_{run_id}_{first_trade_id}_{last_trade_id_in_batch}_{len(cached_trades)}.jsonl"
            logger.info(f"Writing final batch for {product_id}: {len(cached_trades)} trades to s3://{bucket}/{key}")
            write_jsonl_s3(cached_records, bucket=bucket, key=key)
            
            if checkpoint_mgr:
                checkpoint_mgr.save(product_id, {"last_trade_id": new_last_trade_id, "trades_processed": trades_fetched})
                logger.debug(f"Checkpoint saved for {product_id}: {trades_fetched} trades processed")
            
            logger.info(f"{product_id}: wrote final {len(cached_trades)} trades to S3 (total: {trades_fetched})")
            print(f"  {product_id}: wrote final {len(cached_trades)} trades to S3 (total: {trades_fetched})")
        
        logger.info(f"Backfill complete for {product_id}: {trades_fetched} total trades")
        return {"product": product_id, "status": "ok", "trades_fetched": trades_fetched}
    except Exception as e:
        logger.error(f"Error during backfill of {product_id}: {e}", exc_info=True)
        return {"product": product_id, "status": "error", "error": str(e)}


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
