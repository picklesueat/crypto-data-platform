"""Base interface for exchange connectors."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Tuple


class ExchangeConnectorInterface(ABC):
    """Abstract base class defining the interface for exchange connectors.

    This interface allows for multiple implementations (custom, CCXT-based, etc.)
    while maintaining consistent behavior across the system.
    """

    @abstractmethod
    def fetch_trades_with_cursor(
        self,
        product_id: str,
        limit: int = 1000,
        before: Optional[int] = None,
        after: Optional[int] = None,
        **kwargs
    ) -> Tuple[List, Optional[int]]:
        """Fetch trades with pagination cursor.

        Args:
            product_id: Exchange-specific product/trading pair identifier
                       (e.g., "BTC-USD" for Coinbase, "BTC/USD" for CCXT exchanges)
            limit: Maximum number of trades to fetch per request
            before: Fetch trades before this cursor (pagination backward in time)
            after: Fetch trades after this cursor (pagination forward in time)
            **kwargs: Additional exchange-specific parameters (timeout, max_retries, etc.)

        Returns:
            Tuple of (trades list, next_cursor)
            - trades: List of trade objects (implementation-specific type)
            - next_cursor: Cursor value for next page, or None if no more data

        Raises:
            ValueError: If both before and after are provided
            requests.HTTPError: If API returns error status code
            requests.Timeout: If request times out
            requests.ConnectionError: If network connection fails
        """
        pass

    @abstractmethod
    def get_latest_trade_id(self, product_id: str, **kwargs) -> int:
        """Get the most recent trade ID for a product.

        Used for initializing watermarks and determining backfill starting points.

        Args:
            product_id: Exchange-specific product identifier
            **kwargs: Additional parameters (timeout, etc.)

        Returns:
            The most recent trade_id for this product

        Raises:
            requests.RequestException: If API call fails
            ValueError: If no trades returned
        """
        pass

    @abstractmethod
    def to_raw_record(
        self,
        trade,
        product_id: str,
        ingest_ts: datetime
    ) -> dict:
        """Convert a trade object to the unified raw record format.

        This method normalizes exchange-specific trade formats into the common
        schema used for JSONL storage in S3.

        Args:
            trade: Exchange-specific trade object (CoinbaseTrade, CCXT dict, etc.)
            product_id: Exchange-specific product identifier
            ingest_ts: Timestamp when this trade was ingested

        Returns:
            Dictionary with the following schema:
            {
                "trade_id": str,           # Unique trade identifier
                "product_id": str,         # Trading pair (e.g., "BTC-USD")
                "price": float,            # Trade price
                "size": float,             # Trade quantity
                "time": datetime,          # Trade execution timestamp (UTC)
                "side": str,               # "BUY" or "SELL" (uppercase)
                "_source": str,            # Exchange name (e.g., "coinbase")
                "_source_ingest_ts": datetime,  # Ingestion timestamp
                "_raw_payload": str        # JSON-serialized original payload
            }
        """
        pass


__all__ = ["ExchangeConnectorInterface"]
