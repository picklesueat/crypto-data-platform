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

from dotenv import load_dotenv

from schemahub.connectors.coinbase import CoinbaseConnector
from schemahub.raw_writer import write_jsonl_s3
from schemahub.checkpoint import CheckpointManager

# Load .env file if it exists
load_dotenv()


def ingest_coinbase(
    product_id: str,
    limit: int,
    bucket: str,
    prefix: str,
    before: int | None,
    after: int | None,
) -> str:
    connector = CoinbaseConnector()
    ingest_ts = datetime.now(timezone.utc)
    trades = list(connector.fetch_trades(product_id=product_id, limit=limit, before=before, after=after))
    raw_records = [connector.to_raw_record(trade, product_id, ingest_ts) for trade in trades]
    key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{ingest_ts:%Y%m%dT%H%M%SZ}.jsonl"
    write_jsonl_s3(raw_records, bucket=bucket, key=key)
    return key


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SchemaHub CLI (Coinbase-only MVP)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest from Coinbase")
    ingest_parser.add_argument("product", nargs="?", help="Coinbase product id, e.g. BTC-USD. If omitted, use seed file")
    ingest_parser.add_argument("--seed-path", default=None, help="Optional seed file path (default config/mappings/product_ids_seed.yaml)")
    ingest_parser.add_argument("--limit", type=int, default=100, help="Number of trades to request (max 100)")
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
    backfill.add_argument("--chunk-size", type=int, default=100, help="Number of trades per request (default 100)")
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

        if not products_to_run:
            print("No products to ingest. Provide a product or a seed file.", file=sys.stderr)
            sys.exit(2)

        # Checkpoint manager is always enabled (watermark pattern)
        checkpoint_mgr = CheckpointManager(
            s3_bucket=s3_bucket,
            s3_prefix=args.s3_prefix,
            use_s3=True,
        )

        for pid in products_to_run:
            # Load watermark/checkpoint unless --skip-checkpoint is set
            before = args.before
            if not args.skip_checkpoint:
                ckpt = checkpoint_mgr.load(pid)
                if ckpt:
                    before = ckpt.get("last_trade_id")
                    print(f"  Resuming {pid} from trade_id {before}")

            key = ingest_coinbase(
                product_id=pid,
                limit=args.limit,
                bucket=s3_bucket,
                prefix=args.s3_prefix,
                before=before,
                after=args.after,
            )
            print(f"Wrote Coinbase trades for {pid} to s3://{s3_bucket}/{key}")

            # Update watermark with last trade_id
            if key:
                # Parse the last trade_id from the key (format: raw_coinbase_trades_{product_id}_{timestamp}_{trade_id}.jsonl)
                try:
                    last_trade_id = int(key.split("_")[-1].replace(".jsonl", ""))
                    checkpoint_mgr.save(pid, {"last_trade_id": last_trade_id})
                except (ValueError, IndexError):
                    pass  # If we can't parse trade_id from key, skip checkpoint update

    if args.command == "update-seed":
        connector = CoinbaseConnector()
        try:
            resp = requests.get("https://api.exchange.coinbase.com/products", timeout=10)
            resp.raise_for_status()
            products = resp.json()
        except Exception as exc:  # noqa: BLE001 - keep simple
            print(f"Failed to fetch products from Coinbase: {exc}", file=sys.stderr)
            sys.exit(2)

        ids = sorted({p.get("id") for p in products if p.get("id")})

        if args.filter_regex:
            rx = re.compile(args.filter_regex)
            ids = [i for i in ids if rx.search(i)]

        if args.merge:
            existing, _meta = connector.load_product_seed(args.path)
            ids = sorted(set(existing) | set(ids))

        if args.dry_run:
            print(f"Would write {len(ids)} product ids to {args.path or 'DEFAULT'}")
            for i in ids[:200]:
                print(i)
            if len(ids) > 200:
                print("... (truncated)")
            return

        metadata = {"source": "coinbase", "count": len(ids)}
        connector.save_product_seed(ids, path=args.path, metadata=metadata)
        saved, meta = connector.load_product_seed(args.path)
        print(f"Wrote {len(saved)} product ids to {args.path or 'DEFAULT'}")

    if args.command == "backfill":
        s3_bucket = get_s3_bucket(args)
        connector = CoinbaseConnector()
        products, _meta = connector.load_product_seed(path=args.seed_path)
        
        if not products:
            print("No products in seed file. Run 'update-seed' first.", file=sys.stderr)
            sys.exit(2)
        
        checkpoint_mgr = CheckpointManager(
            s3_bucket=s3_bucket,
            s3_prefix=args.s3_prefix,
            use_s3=args.checkpoint_s3,
        ) if args.resume else None
        
        if args.dry_run:
            print(f"[DRY-RUN] Would backfill {len(products)} products:")
            for pid in products:
                backfill_product(pid, args.chunk_size, s3_bucket, args.s3_prefix, checkpoint_mgr, dry_run=True)
            return
        
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
                    False,
                ): pid for pid in products
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                status = result.get("status", "unknown")
                trades = result.get("trades_fetched", 0)
                print(f"Backfill {result['product']}: {status} ({trades} trades)")
        
        ok_count = sum(1 for r in results if r.get("status") == "ok")
        print(f"\nBackfill complete: {ok_count}/{len(products)} products successful")


def backfill_product(product_id: str, chunk_size: int, bucket: str, prefix: str, checkpoint_mgr: CheckpointManager, dry_run: bool = False) -> dict:
    """Backfill trades for a single product."""
    connector = CoinbaseConnector()
    
    # Load checkpoint to resume
    ckpt = checkpoint_mgr.load(product_id) if checkpoint_mgr else {}
    last_trade_id = ckpt.get("last_trade_id")
    
    if dry_run:
        print(f"[DRY-RUN] Would backfill {product_id} from trade_id {last_trade_id or 'beginning'}")
        return {"product": product_id, "status": "dry-run"}
    
    try:
        ingest_ts = datetime.now(timezone.utc)
        before = last_trade_id
        trades_fetched = 0
        
        while True:
            page_trades = list(connector.fetch_trades(
                product_id=product_id,
                limit=chunk_size,
                before=before,
            ))
            
            if not page_trades:
                break
            
            trades_fetched += len(page_trades)
            raw_records = [connector.to_raw_record(t, product_id, ingest_ts) for t in page_trades]
            key = f"{prefix.rstrip('/')}/raw_coinbase_trades_{product_id}_{ingest_ts:%Y%m%dT%H%M%SZ}_{page_trades[-1].trade_id}.jsonl"
            write_jsonl_s3(raw_records, bucket=bucket, key=key)
            
            # Update checkpoint
            new_last_trade_id = page_trades[-1].trade_id
            if checkpoint_mgr:
                checkpoint_mgr.save(product_id, {"last_trade_id": new_last_trade_id, "trades_processed": trades_fetched})
            
            before = new_last_trade_id
            print(f"  {product_id}: wrote {len(page_trades)} trades (total: {trades_fetched})")
        
        return {"product": product_id, "status": "ok", "trades_fetched": trades_fetched}
    except Exception as e:
        return {"product": product_id, "status": "error", "error": str(e)}


if __name__ == "__main__":
    main()
