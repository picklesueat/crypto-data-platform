"""Simple CLI for Coinbase ingestion."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable
import re
import sys
import requests

from schemahub.connectors.coinbase import CoinbaseConnector
from schemahub.raw_writer import write_jsonl_s3


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
    ingest_parser.add_argument("--s3-bucket", required=True, help="S3 bucket for raw Coinbase trades")
    ingest_parser.add_argument(
        "--s3-prefix",
        default="schemahub/raw_coinbase_trades",
        help="S3 key prefix for raw Coinbase trades",
    )

    # Simple update-seed command (barebones)
    upd = subparsers.add_parser("update-seed", help="Fetch product ids from Coinbase and update seed file")
    upd.add_argument("--path", default=None, help="Path to seed YAML (default config/mappings/product_ids_seed.yaml)")
    upd.add_argument("--merge", action="store_true", help="Merge fetched ids with existing seed file instead of replacing")
    upd.add_argument("--filter-regex", default=None, help="Only keep product ids matching this regex, e.g. '.*-USD'")
    upd.add_argument("--dry-run", action="store_true", help="Print what would be written but do not write file")

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
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

        for pid in products_to_run:
            key = ingest_coinbase(
                product_id=pid,
                limit=args.limit,
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                before=args.before,
                after=args.after,
            )
            print(f"Wrote Coinbase trades for {pid} to s3://{args.s3_bucket}/{key}")

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


if __name__ == "__main__":
    main()
