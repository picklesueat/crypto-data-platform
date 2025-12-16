"""Coinbase connector for fetching recent trades."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple, Iterable as _Iterable

import requests
from dotenv import load_dotenv
import yaml

COINBASE_API_URL = "https://api.exchange.coinbase.com"

# Default path for product seed file (config/mappings/product_ids_seed.yaml)
DEFAULT_SEED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config",
    "mappings",
    "product_ids_seed.yaml",
)

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
        message = f"{timestamp}GET/products"  # Placeholder; dynamic paths should update this
        signature = hmac.new(
            secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')

    def fetch_trades(
        self,
        product_id: str,
        limit: int = 1000,
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

    # --- Seed file utilities -------------------------------------------------
    @staticmethod
    def load_product_seed(path: Optional[str] = None) -> Tuple[List[str], dict]:
        """Load product ids and metadata from a YAML seed file.

        Returns a tuple of (product_ids, metadata). If the file does not exist,
        returns an empty list and empty metadata.
        """
        path = path or DEFAULT_SEED_PATH
        if not os.path.exists(path):
            return [], {}
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        product_ids = data.get("product_ids") or []
        metadata = data.get("metadata") or {}
        return product_ids, metadata

    @staticmethod
    def save_product_seed(product_ids: _Iterable[str], path: Optional[str] = None, metadata: Optional[dict] = None) -> None:
        """Save product ids and optional metadata to a YAML seed file.

        This will create parent directories if necessary and write atomically
        by writing to a temporary file then renaming.
        """
        path = path or DEFAULT_SEED_PATH
        parent = os.path.dirname(path)
        os.makedirs(parent, exist_ok=True)
        payload = {
            "product_ids": list(product_ids),
            "metadata": dict(metadata or {}),
        }
        payload["metadata"].setdefault("last_updated", datetime.utcnow().isoformat() + "Z")

        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(payload, fh, sort_keys=False)
        os.replace(tmp_path, path)

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
