"""Simple CLI for Coinbase ingestion."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from schemahub.connectors.coinbase import CoinbaseConnector
from schemahub.raw_writer import write_jsonl


def ingest_coinbase(product_id: str, limit: int, output: Path, before: int | None, after: int | None) -> int:
    connector = CoinbaseConnector()
    ingest_ts = datetime.now(timezone.utc)
    trades = list(connector.fetch_trades(product_id=product_id, limit=limit, before=before, after=after))
    raw_records = [connector.to_raw_record(trade, product_id, ingest_ts) for trade in trades]
    write_jsonl(raw_records, output)
    return len(raw_records)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SchemaHub CLI (Coinbase-only MVP)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest from Coinbase")
    ingest_parser.add_argument("product", help="Coinbase product id, e.g. BTC-USD")
    ingest_parser.add_argument("--limit", type=int, default=100, help="Number of trades to request (max 100)")
    ingest_parser.add_argument("--before", type=int, default=None, help="Paginate using a trade_id upper bound")
    ingest_parser.add_argument("--after", type=int, default=None, help="Paginate using a trade_id lower bound")
    ingest_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw_coinbase_trades.jsonl"),
        help="Where to write the raw Coinbase trades",
    )

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        count = ingest_coinbase(
            product_id=args.product,
            limit=args.limit,
            output=args.output,
            before=args.before,
            after=args.after,
        )
        print(f"Wrote {count} trades to {args.output}")


if __name__ == "__main__":
    main()
