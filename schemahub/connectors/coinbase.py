"""Coinbase connector for fetching recent trades."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple, Iterable as _Iterable

import requests
from dotenv import load_dotenv
import yaml

from schemahub.health import get_circuit_breaker
from schemahub.metrics import get_metrics_client
from schemahub.rate_limiter import get_rate_limiter

logger = logging.getLogger(__name__)

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
            logger.debug("Using authenticated Coinbase API credentials")
            self.session.headers.update({
                "CB-ACCESS-KEY": api_key,
                "CB-ACCESS-SIGN": self._generate_signature(api_secret),
                "CB-ACCESS-TIMESTAMP": str(int(datetime.now().timestamp())),
            })
        else:
            logger.debug("Using public Coinbase API (no authentication)")

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

    def fetch_trades_with_cursor(
        self,
        product_id: str,
        limit: int = 1000,
        before: Optional[int] = None,
        after: Optional[int] = None,
        max_retries: int = 3,
        timeout: int = 30,
    ) -> Tuple[List[CoinbaseTrade], Optional[int]]:
        """Fetch trades and return the cursor from CB-AFTER header for next pagination.

        Includes retry logic with exponential backoff for timeouts.

        Args:
            timeout: Read timeout in seconds (default: 15). Increase if getting timeout errors.

        Returns:
            Tuple of (trades list, next_after_cursor from CB-AFTER header)
        """
        if before is not None and after is not None:
            raise ValueError("Only one of 'before' or 'after' may be provided")

        params = {"limit": limit}
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after

        url = f"{COINBASE_API_URL}/products/{product_id}/trades"
        logger.info(f"[API] {product_id}: GET {url} params={params} timeout={timeout}s")

        # Get circuit breaker and metrics client
        circuit_breaker = get_circuit_breaker()
        metrics = get_metrics_client()

        last_error = None
        for attempt in range(1, max_retries + 1):
            # Check circuit state and get wait time
            wait_time = circuit_breaker.get_wait_time("coinbase", attempt)
            if wait_time > 0:
                logger.warning(f"[API] {product_id}: Circuit-aware wait: {wait_time}s before attempt {attempt}/{max_retries}")
                time.sleep(wait_time)

            start_time = time.time()
            response = None

            try:
                # Acquire rate limit token before making API request
                rate_limiter = get_rate_limiter("coinbase")
                logger.debug(f"[API] {product_id}: Acquiring rate limit token (attempt {attempt}/{max_retries})")
                rate_limiter.acquire()  # Blocks if rate limit reached
                logger.debug(f"[API] {product_id}: Rate limit token acquired")

                logger.info(f"[API] {product_id}: Attempt {attempt}/{max_retries}, timeout={timeout}s")
                response = self.session.get(url, params=params, timeout=timeout)
                elapsed_ms = (time.time() - start_time) * 1000
                logger.info(f"[API] {product_id}: Response received in {elapsed_ms:.0f}ms, status={response.status_code}")
                logger.debug(f"[API] {product_id}: Response headers: Content-Length={response.headers.get('Content-Length')}, Content-Type={response.headers.get('Content-Type')}")

                # Check status FIRST before recording success
                response.raise_for_status()

                # Now record SUCCESS (only if status check passed)
                circuit_breaker.record_success("coinbase", elapsed_ms)
                metrics.put_exchange_response_time("coinbase", elapsed_ms)
                metrics.put_exchange_status("coinbase", is_healthy=True)
                metrics.put_api_success("coinbase")

                break  # Success, exit retry loop

            except requests.exceptions.Timeout as e:
                elapsed_ms = (time.time() - start_time) * 1000
                last_error = e
                error_msg = f"Timeout after {elapsed_ms:.0f}ms"

                # Record FAILURE
                circuit_breaker.record_failure("coinbase", error_msg)
                metrics.put_exchange_status("coinbase", is_healthy=False)
                metrics.put_timeout_error("coinbase")

                logger.error(f"[API] {product_id}: TIMEOUT after {elapsed_ms:.0f}ms on attempt {attempt}/{max_retries}: {e}")
                logger.error(f"[API] {product_id}: Timeout Type: Read timeout (taking too long to receive data)")

                if attempt < max_retries:
                    logger.info(f"[API] {product_id}: Will retry (attempt {attempt+1}/{max_retries})")
                    continue
                else:
                    logger.error(f"[API] {product_id}: FAILED after {max_retries} attempts")
                    raise

            except requests.exceptions.ConnectTimeout as e:
                elapsed_ms = (time.time() - start_time) * 1000
                last_error = e
                error_msg = f"ConnectTimeout after {elapsed_ms:.0f}ms"

                circuit_breaker.record_failure("coinbase", error_msg)
                metrics.put_exchange_status("coinbase", is_healthy=False)
                metrics.put_timeout_error("coinbase")

                logger.error(f"[API] {product_id}: CONNECTION TIMEOUT after {elapsed_ms:.0f}ms: {e}")
                raise

            except requests.exceptions.ConnectionError as e:
                elapsed_ms = (time.time() - start_time) * 1000
                last_error = e
                error_msg = f"ConnectionError after {elapsed_ms:.0f}ms"

                circuit_breaker.record_failure("coinbase", error_msg)
                metrics.put_exchange_status("coinbase", is_healthy=False)
                metrics.put_connection_error("coinbase")

                logger.error(f"[API] {product_id}: CONNECTION ERROR after {elapsed_ms:.0f}ms: {e}")
                raise

            except requests.exceptions.HTTPError as e:
                elapsed_ms = (time.time() - start_time) * 1000
                last_error = e
                error_response = e.response if hasattr(e, 'response') else response

                if error_response is not None:
                    status_code = error_response.status_code
                    error_msg = f"HTTP {status_code}"

                    # Record FAILURE
                    circuit_breaker.record_failure("coinbase", error_msg)
                    metrics.put_exchange_status("coinbase", is_healthy=False)

                    logger.error(f"[API] {product_id}: *** HTTP ERROR *** status={status_code} after {elapsed_ms:.0f}ms: {e}")

                    if status_code == 429:
                        logger.error(f"[API] {product_id}: RATE LIMITED! (HTTP 429)")
                        metrics.put_rate_limit_error("coinbase")
                        if attempt < max_retries:
                            backoff_seconds = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                            logger.error(f"[API] {product_id}: Backing off {backoff_seconds}s before retry (attempt {attempt+1}/{max_retries})")
                            time.sleep(backoff_seconds)
                            continue
                        else:
                            logger.error(f"[API] {product_id}: FAILED after {max_retries} attempts (rate limit)")
                            raise

                    elif status_code >= 500:
                        logger.error(f"[API] {product_id}: *** SERVER ERROR 5XX DETECTED *** status={status_code}")
                        metrics.put_server_error("coinbase")
                        if attempt < max_retries:
                            backoff_seconds = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                            logger.error(f"[API] {product_id}: Backing off {backoff_seconds}s before retry (attempt {attempt+1}/{max_retries})")
                            time.sleep(backoff_seconds)
                            continue
                        else:
                            logger.error(f"[API] {product_id}: FAILED after {max_retries} attempts (server error)")
                            raise

                    else:
                        # 4xx errors (except 429) are client errors - don't retry
                        logger.error(f"[API] {product_id}: CLIENT ERROR (4xx). Not retrying.")
                        raise
                else:
                    logger.error(f"[API] {product_id}: HTTPError but no response object")
                    raise

            except requests.exceptions.RequestException as e:
                elapsed_ms = (time.time() - start_time) * 1000
                error_msg = f"RequestException: {str(e)[:200]}"

                circuit_breaker.record_failure("coinbase", error_msg)
                metrics.put_exchange_status("coinbase", is_healthy=False)

                logger.error(f"[API] {product_id}: REQUEST EXCEPTION after {elapsed_ms:.0f}ms: {e}", exc_info=True)
                raise
        
        # Parse response
        try:
            parse_start = time.time()
            payloads: List[dict] = response.json()
            parse_elapsed = time.time() - parse_start
            logger.debug(f"[API] {product_id}: Parsed {len(payloads)} trades in {parse_elapsed:.3f}s")
        except Exception as e:
            logger.error(f"[API] {product_id}: Failed to parse JSON response: {e}")
            logger.debug(f"[API] {product_id}: Response text (first 500 chars): {response.text[:500]}")
            raise
        
        trades = [CoinbaseTrade.from_payload(payload) for payload in payloads]
        logger.debug(f"[API] {product_id}: Created {len(trades)} CoinbaseTrade objects")
        
        # Get the next cursor from the CB-AFTER header if it exists
        next_cursor = response.headers.get("CB-AFTER")
        if next_cursor:
            try:
                next_cursor = int(next_cursor)
            except (ValueError, TypeError):
                logger.warning(f"[API] {product_id}: Could not parse CB-AFTER header: {response.headers.get('CB-AFTER')}")
                next_cursor = None
        
        total_elapsed = time.time() - start_time
        logger.info(f"[API] {product_id}: SUCCESS - {len(trades)} trades in {total_elapsed:.2f}s, next_cursor={next_cursor}")
        return trades, next_cursor

    # --- Seed file utilities -------------------------------------------------
    def get_latest_trade_id(self, product_id: str, timeout: int = 15) -> int:
        """Get the latest (most recent) trade ID for a product.
        
        Args:
            product_id: Coinbase product ID (e.g., BTC-USD)
            timeout: Request timeout in seconds
            
        Returns:
            The most recent trade_id for this product
            
        Raises:
            requests.RequestException: If API call fails
            ValueError: If no trades returned
        """
        url = f"{COINBASE_API_URL}/products/{product_id}/trades"
        params = {"limit": 1}

        # Acquire rate limit token before making API request
        rate_limiter = get_rate_limiter("coinbase")
        logger.debug(f"[API] {product_id}: Acquiring rate limit token for get_latest_trade_id")
        rate_limiter.acquire()

        logger.info(f"[API] {product_id}: Fetching latest trade_id")
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        
        payloads = response.json()
        if not payloads:
            raise ValueError(f"No trades returned for {product_id}")
        
        latest_trade_id = payloads[0]["trade_id"]
        logger.info(f"[API] {product_id}: Latest trade_id = {latest_trade_id}")
        return latest_trade_id

    @staticmethod
    def load_product_seed(path: Optional[str] = None) -> Tuple[List[str], dict]:
        """Load product ids and metadata from a YAML seed file.

        Returns a tuple of (product_ids, metadata). If the file does not exist,
        returns an empty list and empty metadata.
        """
        path = path or DEFAULT_SEED_PATH
        if not os.path.exists(path):
            logger.warning(f"Product seed file not found: {path}")
            return [], {}
        
        logger.info(f"Loading product seed from {path}")
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        product_ids = data.get("product_ids") or []
        metadata = data.get("metadata") or {}
        logger.info(f"Loaded {len(product_ids)} products from seed file")
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
        logger.info(f"Saving {len(list(product_ids))} product IDs to {path}")
        
        payload = {
            "product_ids": list(product_ids),
            "metadata": dict(metadata or {}),
        }
        payload["metadata"].setdefault("last_updated", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))

        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(payload, fh, sort_keys=False)
        os.replace(tmp_path, path)
        logger.info(f"Successfully saved seed file to {path}")

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


__all__ = ["CoinbaseConnector", "CoinbaseTrade", "_parse_time"]
