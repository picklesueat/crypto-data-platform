from datetime import datetime, timezone

import pytest

from schemahub.connectors.coinbase import CoinbaseConnector, CoinbaseTrade


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {}
        self.last_params = None

    def get(self, url, params=None, timeout=None):
        self.last_params = params
        return DummyResponse(self.payload)


@pytest.fixture
def sample_payload():
    return [
        {
            "trade_id": 123,
            "price": "35000.12",
            "size": "0.5",
            "time": "2024-06-01T12:00:00.123456Z",
            "side": "buy",
        }
    ]


def test_fetch_trades_uses_params(sample_payload):
    session = DummySession(sample_payload)
    connector = CoinbaseConnector(session=session)

    trades = list(connector.fetch_trades("BTC-USD", limit=50, before=10))

    assert session.last_params == {"limit": 50, "before": 10}
    assert len(trades) == 1
    assert isinstance(trades[0], CoinbaseTrade)
    assert trades[0].trade_id == 123


def test_to_raw_record_converts_fields(sample_payload):
    trade = CoinbaseTrade.from_payload(sample_payload[0])
    ingest_ts = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)

    record = CoinbaseConnector.to_raw_record(trade, "BTC-USD", ingest_ts)

    assert record["trade_id"] == "123"
    assert record["product_id"] == "BTC-USD"
    assert record["price"] == pytest.approx(35000.12)
    assert record["size"] == pytest.approx(0.5)
    assert record["time"].isoformat() == "2024-06-01T12:00:00.123456+00:00"
    assert record["side"] == "BUY"
    assert record["_source"] == "coinbase"
    assert record["_source_ingest_ts"] == ingest_ts
    assert "_raw_payload" in record


def test_fetch_trades_rejects_conflicting_cursors(sample_payload):
    session = DummySession(sample_payload)
    connector = CoinbaseConnector(session=session)

    with pytest.raises(ValueError):
        list(connector.fetch_trades("BTC-USD", before=1, after=2))
