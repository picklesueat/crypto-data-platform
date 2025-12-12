"""Coinbase connector for fetching recent trades."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import requests
from dotenv import load_dotenv

COINBASE_API_URL = "https://api.exchange.coinbase.com"

# Load environment variables from .env file
load_dotenv()


@dataclass
class CoinbaseTrade:
    """Represents a single trade payload from Coinbase."""

    trade_id: int
    price: str
    size: str
    time: str
    side: str
    bid: Optional[float] = None
    ask: Optional[float] = None

    @classmethod
    def from_payload(cls, payload: dict) -> "CoinbaseTrade":
        return cls(
            trade_id=payload["trade_id"],
            price=payload["price"],
            size=payload["size"],
            time=payload["time"],
            side=payload["side"],
            bid=payload.get("bid"),
            ask=payload.get("ask"),
        )


class CoinbaseConnector:
    """Fetches trades from the Coinbase public REST API."""

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "schemahub/0.1",
        })

        # Add authentication headers if API credentials are provided
        api_key = os.getenv("COINBASE_API_KEY")
        api_secret = os.getenv("COINBASE_API_SECRET")
        if api_key and api_secret:
            self.session.headers.update({
                "CB-ACCESS-KEY": api_key,
                "CB-ACCESS-SIGN": self._generate_signature(api_secret),
                "CB-ACCESS-TIMESTAMP": str(int(datetime.now().timestamp())),
            })

    def _generate_signature(self, secret: str) -> str:
        """Generate a signature for authenticated requests."""
        timestamp = str(int(datetime.now().timestamp()))
        message = f"{timestamp}GET/products/BTC-USD/trades"  # Example path, adjust dynamically
        signature = hmac.new(
            secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')

    def fetch_trades(
        self,
        product_id: str,
        limit: int = 100,
        before: Optional[int] = None,
        after: Optional[int] = None,
    ) -> Iterable[CoinbaseTrade]:
        """Fetch the latest trades for a product.

        The Coinbase API returns trades in descending order by trade_id. The
        ``before`` and ``after`` parameters allow cursor-based pagination using
        the trade_id value.
        """

        if before is not None and after is not None:
            raise ValueError("Only one of 'before' or 'after' may be provided")

        params = {"limit": limit}
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after

        url = f"{COINBASE_API_URL}/products/{product_id}/trades"
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        payloads: List[dict] = response.json()
        for payload in payloads:
            yield CoinbaseTrade.from_payload(payload)

    @staticmethod
    def to_raw_record(
        trade: CoinbaseTrade, product_id: str, ingest_ts: datetime
    ) -> dict:
        """Convert a :class:`CoinbaseTrade` into the raw table schema."""

        parsed_time = _parse_time(trade.time)
        return {
            "trade_id": str(trade.trade_id),
            "product_id": product_id,
            "price": float(trade.price),
            "size": float(trade.size),
            "time": parsed_time,
            "side": trade.side.upper(),
            "_source": "coinbase",
            "_source_ingest_ts": ingest_ts,
            "_raw_payload": json.dumps(trade.__dict__),
        }


def _parse_time(value: str) -> datetime:
    """Parse an ISO8601 timestamp returned by Coinbase."""

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


__all__ = ["CoinbaseConnector", "CoinbaseTrade"]
