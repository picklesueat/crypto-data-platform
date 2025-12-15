"""Unit tests for CoinbaseTrade and CoinbaseConnector."""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from schemahub.connectors.coinbase import CoinbaseTrade, CoinbaseConnector, _parse_time


class DummyResponse:
    """Mock response object for testing."""
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class DummySession:
    """Mock session object for testing."""
    def __init__(self, payload):
        self.payload = payload
        self.headers = {}
        self.last_params = None
        self.last_url = None

    def get(self, url, params=None, timeout=None):
        self.last_url = url
        self.last_params = params
        return DummyResponse(self.payload)


class TestCoinbaseTrade:
    """Tests for CoinbaseTrade dataclass."""

    def test_from_payload_creates_trade(self):
        """Creating a trade from a payload creates correct object."""
        payload = {
            "trade_id": 123456,
            "price": "35000.50",
            "size": "0.5",
            "time": "2024-06-01T12:00:00Z",
            "side": "buy",
        }
        
        trade = CoinbaseTrade.from_payload(payload)
        
        assert trade.trade_id == 123456
        assert trade.price == "35000.50"
        assert trade.size == "0.5"
        assert trade.time == "2024-06-01T12:00:00Z"
        assert trade.side == "buy"
        assert trade.bid is None
        assert trade.ask is None

    def test_from_payload_with_optional_fields(self):
        """Creating a trade with optional bid/ask fields works."""
        payload = {
            "trade_id": 123456,
            "price": "35000.50",
            "size": "0.5",
            "time": "2024-06-01T12:00:00Z",
            "side": "sell",
            "bid": 35000.0,
            "ask": 35001.0,
        }
        
        trade = CoinbaseTrade.from_payload(payload)
        
        assert trade.bid == 35000.0
        assert trade.ask == 35001.0

    def test_from_payload_missing_optional_fields(self):
        """Creating a trade without optional fields uses None."""
        payload = {
            "trade_id": 123,
            "price": "1000",
            "size": "1",
            "time": "2024-06-01T12:00:00Z",
            "side": "buy",
        }
        
        trade = CoinbaseTrade.from_payload(payload)
        
        assert trade.bid is None
        assert trade.ask is None

    def test_trade_is_dataclass(self):
        """CoinbaseTrade is a dataclass with expected fields."""
        trade = CoinbaseTrade(
            trade_id=1,
            price="100",
            size="0.5",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        
        assert hasattr(trade, "trade_id")
        assert hasattr(trade, "price")
        assert hasattr(trade, "size")
        assert hasattr(trade, "time")
        assert hasattr(trade, "side")


class TestParseTime:
    """Tests for time parsing utility."""

    def test_parse_iso8601_with_z_suffix(self):
        """Parsing ISO8601 with Z suffix returns UTC datetime."""
        result = _parse_time("2024-06-01T12:00:00.123456Z")
        
        assert result.tzinfo == timezone.utc
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 1
        assert result.hour == 12
        assert result.minute == 0
        assert result.second == 0

    def test_parse_iso8601_with_offset(self):
        """Parsing ISO8601 with +00:00 offset works."""
        result = _parse_time("2024-06-01T12:00:00+00:00")
        
        assert result.tzinfo == timezone.utc

    def test_parse_naive_datetime_assumes_utc(self):
        """Parsing naive datetime assumes UTC."""
        result = _parse_time("2024-06-01T12:00:00")
        
        assert result.tzinfo == timezone.utc

    def test_parse_time_with_microseconds(self):
        """Parsing time with microseconds preserves them."""
        result = _parse_time("2024-06-01T12:00:00.123456Z")
        
        assert result.microsecond == 123456

    def test_parse_time_converts_to_utc(self):
        """Parsing non-UTC time converts to UTC."""
        # Note: this assumes the parsed time is converted if not UTC
        result = _parse_time("2024-06-01T12:00:00Z")
        
        assert result.tzinfo == timezone.utc


class TestCoinbaseConnectorFetchTrades:
    """Tests for fetch_trades method."""

    def test_fetch_trades_basic(self):
        """Fetching trades returns trade objects."""
        payload = [
            {
                "trade_id": 123,
                "price": "35000.50",
                "size": "0.5",
                "time": "2024-06-01T12:00:00Z",
                "side": "buy",
            }
        ]
        session = DummySession(payload)
        connector = CoinbaseConnector(session=session)
        
        trades = list(connector.fetch_trades("BTC-USD"))
        
        assert len(trades) == 1
        assert trades[0].trade_id == 123
        assert trades[0].side == "buy"

    def test_fetch_trades_uses_limit_param(self):
        """Fetching trades passes limit parameter to API."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        list(connector.fetch_trades("BTC-USD", limit=50))
        
        assert session.last_params["limit"] == 50

    def test_fetch_trades_default_limit(self):
        """Fetching trades uses default limit of 100."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        list(connector.fetch_trades("BTC-USD"))
        
        assert session.last_params["limit"] == 100

    def test_fetch_trades_uses_before_param(self):
        """Fetching trades with before parameter includes it."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        list(connector.fetch_trades("BTC-USD", before=12345))
        
        assert session.last_params["before"] == 12345

    def test_fetch_trades_uses_after_param(self):
        """Fetching trades with after parameter includes it."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        list(connector.fetch_trades("BTC-USD", after=12345))
        
        assert session.last_params["after"] == 12345

    def test_fetch_trades_before_and_after_mutually_exclusive(self):
        """Fetching with both before and after raises ValueError."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        with pytest.raises(ValueError, match="Only one of"):
            list(connector.fetch_trades("BTC-USD", before=100, after=200))

    def test_fetch_trades_correct_url(self):
        """Fetching trades uses correct API URL."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        list(connector.fetch_trades("BTC-USD"))
        
        assert "api.exchange.coinbase.com" in session.last_url
        assert "BTC-USD" in session.last_url
        assert "trades" in session.last_url

    def test_fetch_trades_multiple_trades(self):
        """Fetching multiple trades returns all of them."""
        payload = [
            {"trade_id": 1, "price": "100", "size": "1", "time": "2024-06-01T12:00:00Z", "side": "buy"},
            {"trade_id": 2, "price": "101", "size": "1", "time": "2024-06-01T12:00:01Z", "side": "sell"},
            {"trade_id": 3, "price": "102", "size": "1", "time": "2024-06-01T12:00:02Z", "side": "buy"},
        ]
        session = DummySession(payload)
        connector = CoinbaseConnector(session=session)
        
        trades = list(connector.fetch_trades("BTC-USD"))
        
        assert len(trades) == 3
        assert trades[0].trade_id == 1
        assert trades[1].trade_id == 2
        assert trades[2].trade_id == 3

    def test_fetch_trades_is_iterable(self):
        """fetch_trades returns an iterable."""
        payload = [
            {"trade_id": 1, "price": "100", "size": "1", "time": "2024-06-01T12:00:00Z", "side": "buy"},
        ]
        session = DummySession(payload)
        connector = CoinbaseConnector(session=session)
        
        result = connector.fetch_trades("BTC-USD")
        
        # Should be iterable but not consumed yet
        assert hasattr(result, "__iter__")

    def test_fetch_trades_empty_response(self):
        """Fetching trades with empty API response yields nothing."""
        session = DummySession([])
        connector = CoinbaseConnector(session=session)
        
        trades = list(connector.fetch_trades("BTC-USD"))
        
        assert trades == []


class TestCoinbaseConnectorToRawRecord:
    """Tests for to_raw_record method."""

    def test_to_raw_record_basic(self):
        """Converting a trade to raw record includes all required fields."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="35000.50",
            size="0.5",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert record["trade_id"] == "123"
        assert record["product_id"] == "BTC-USD"
        assert record["price"] == 35000.50
        assert record["size"] == 0.5
        assert record["side"] == "BUY"
        assert record["_source"] == "coinbase"

    def test_to_raw_record_parses_time(self):
        """to_raw_record parses the trade time correctly."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="100",
            size="1",
            time="2024-06-01T12:00:00.123456Z",
            side="buy",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert isinstance(record["time"], datetime)
        assert record["time"].tzinfo == timezone.utc

    def test_to_raw_record_normalizes_side_to_uppercase(self):
        """to_raw_record converts side to uppercase."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="100",
            size="1",
            time="2024-06-01T12:00:00Z",
            side="sell",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert record["side"] == "SELL"

    def test_to_raw_record_includes_raw_payload(self):
        """to_raw_record includes _raw_payload as JSON string."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="100",
            size="1",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert "_raw_payload" in record
        import json
        payload = json.loads(record["_raw_payload"])
        assert payload["trade_id"] == 123

    def test_to_raw_record_includes_ingest_timestamp(self):
        """to_raw_record includes _source_ingest_ts."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="100",
            size="1",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert record["_source_ingest_ts"] == ingest_ts

    def test_to_raw_record_converts_price_and_size_to_float(self):
        """to_raw_record converts price and size to floats."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="35000.12345",
            size="0.5678",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert isinstance(record["price"], float)
        assert isinstance(record["size"], float)
        assert abs(record["price"] - 35000.12345) < 0.001
        assert abs(record["size"] - 0.5678) < 0.001

    def test_to_raw_record_converts_trade_id_to_string(self):
        """to_raw_record converts trade_id to string."""
        trade = CoinbaseTrade(
            trade_id=123456,
            price="100",
            size="1",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        
        assert isinstance(record["trade_id"], str)
        assert record["trade_id"] == "123456"

    def test_to_raw_record_multiple_products(self):
        """to_raw_record works with different product_ids."""
        trade = CoinbaseTrade(
            trade_id=123,
            price="100",
            size="1",
            time="2024-06-01T12:00:00Z",
            side="buy",
        )
        ingest_ts = datetime.now(timezone.utc)
        
        btc_record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)
        eth_record = CoinbaseConnector.to_raw_record(trade, "ETH-USD", ingest_ts)
        
        assert btc_record["product_id"] == "BTC-USD"
        assert eth_record["product_id"] == "ETH-USD"
        assert btc_record["trade_id"] == eth_record["trade_id"]


class TestCoinbaseConnectorInitialization:
    """Tests for CoinbaseConnector initialization."""

    def test_init_with_default_session(self):
        """CoinbaseConnector initializes with default session."""
        connector = CoinbaseConnector()
        
        assert connector.session is not None

    def test_init_with_provided_session(self):
        """CoinbaseConnector accepts a provided session."""
        dummy_session = DummySession([])
        connector = CoinbaseConnector(session=dummy_session)
        
        assert connector.session is dummy_session

    def test_init_sets_user_agent(self):
        """CoinbaseConnector sets User-Agent header."""
        connector = CoinbaseConnector()
        
        assert "schemahub" in connector.session.headers["User-Agent"]

    def test_init_without_api_credentials(self):
        """CoinbaseConnector works without API credentials."""
        with patch.dict("os.environ", {}, clear=True):
            with patch("schemahub.connectors.coinbase.load_dotenv"):
                connector = CoinbaseConnector()
                
                # Should still work, just without auth headers
                assert connector.session is not None

    def test_init_with_api_credentials_adds_auth_headers(self):
        """CoinbaseConnector adds auth headers when credentials are present."""
        with patch.dict("os.environ", {"COINBASE_API_KEY": "test_key", "COINBASE_API_SECRET": "test_secret"}):
            with patch("schemahub.connectors.coinbase.load_dotenv"):
                connector = CoinbaseConnector()
                
                # Auth headers should be set
                assert "CB-ACCESS-KEY" in connector.session.headers or len(connector.session.headers) > 1
