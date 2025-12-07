"""Simple CLI for Coinbase ingestion."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable

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
    ingest_parser.add_argument("product", help="Coinbase product id, e.g. BTC-USD")
    ingest_parser.add_argument("--limit", type=int, default=100, help="Number of trades to request (max 100)")
    ingest_parser.add_argument("--before", type=int, default=None, help="Paginate using a trade_id upper bound")
    ingest_parser.add_argument("--after", type=int, default=None, help="Paginate using a trade_id lower bound")
    ingest_parser.add_argument("--s3-bucket", required=True, help="S3 bucket for raw Coinbase trades")
    ingest_parser.add_argument(
        "--s3-prefix",
        default="schemahub/raw_coinbase_trades",
        help="S3 key prefix for raw Coinbase trades",
    )

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        key = ingest_coinbase(
            product_id=args.product,
            limit=args.limit,
            bucket=args.s3_bucket,
            prefix=args.s3_prefix,
            before=args.before,
            after=args.after,
        )
        print(f"Wrote Coinbase trades to s3://{args.s3_bucket}/{key}")


if __name__ == "__main__":
    main()
