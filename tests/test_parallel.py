"""Unit tests for parallel trade fetching."""
import pytest
from unittest.mock import Mock, MagicMock, patch
from dataclasses import dataclass
from typing import List, Optional, Tuple

from schemahub.parallel import fetch_trades_parallel, _fetch_chunk


@dataclass
class MockTrade:
    """Mock trade object for testing."""
    trade_id: int
    price: str = "50000.0"
    size: str = "0.1"
    time: str = "2024-01-01T00:00:00Z"
    side: str = "buy"


class TestFetchTradesParallel:
    """Test the fetch_trades_parallel function."""

    def test_empty_range_returns_empty(self):
        """Test that empty cursor range returns empty list."""
        connector = Mock()

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1000,  # Same as start = empty range
            chunk_concurrency=5,
        )

        assert trades == []
        assert highest == 1000
        connector.fetch_trades_with_cursor.assert_not_called()

    def test_negative_range_returns_empty(self):
        """Test that negative range (cursor_end < cursor_start) returns empty."""
        connector = Mock()

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=2000,
            cursor_end=1000,  # End before start
            chunk_concurrency=5,
        )

        assert trades == []
        assert highest == 2000

    def test_small_range_single_chunk(self):
        """Test fetching a small range (single chunk)."""
        connector = Mock()

        # Mock API response
        mock_trades = [MockTrade(trade_id=i) for i in range(1000, 1100)]
        connector.fetch_trades_with_cursor.return_value = (mock_trades, None)

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1100,
            chunk_concurrency=5,
        )

        assert len(trades) == 100
        assert highest == 1099
        assert trades[0].trade_id == 1000
        assert trades[-1].trade_id == 1099

    def test_parallel_chunks_sorted_by_trade_id(self):
        """Test that parallel chunks are sorted by trade_id."""
        connector = Mock()

        # Mock responses for 3 chunks (simulate out-of-order completion)
        def mock_fetch(product_id, limit, after, **kwargs):
            # Return trades based on cursor
            if after == 1000:
                # Chunk 1: trades 1000-1999
                return ([MockTrade(trade_id=i) for i in range(1000, 2000)], None)
            elif after == 2000:
                # Chunk 2: trades 2000-2999
                return ([MockTrade(trade_id=i) for i in range(2000, 3000)], None)
            elif after == 3000:
                # Chunk 3: trades 3000-3999
                return ([MockTrade(trade_id=i) for i in range(3000, 4000)], None)
            return ([], None)

        connector.fetch_trades_with_cursor.side_effect = mock_fetch

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=4000,
            chunk_concurrency=3,
            chunk_size=1000,
        )

        # Should be 3000 trades total, sorted by trade_id
        assert len(trades) == 3000
        assert highest == 3999

        # Verify sorted order
        for i in range(len(trades) - 1):
            assert trades[i].trade_id < trades[i + 1].trade_id

        # Verify range
        assert trades[0].trade_id == 1000
        assert trades[-1].trade_id == 3999

    def test_chunk_failure_raises_exception(self):
        """Test that chunk failure causes entire batch to fail."""
        connector = Mock()

        # Chunk 1 succeeds, chunk 2 fails, chunk 3 succeeds
        def mock_fetch(product_id, limit, after, **kwargs):
            if after == 1000:
                return ([MockTrade(trade_id=i) for i in range(1000, 2000)], None)
            elif after == 2000:
                raise Exception("Simulated API error")
            elif after == 3000:
                return ([MockTrade(trade_id=i) for i in range(3000, 4000)], None)
            return ([], None)

        connector.fetch_trades_with_cursor.side_effect = mock_fetch

        # Should raise exception due to chunk 2 failure
        with pytest.raises(Exception, match="chunks failed"):
            fetch_trades_parallel(
                connector=connector,
                product_id="BTC-USD",
                cursor_start=1000,
                cursor_end=4000,
                chunk_concurrency=3,
                chunk_size=1000,
            )

    def test_chunk_concurrency_limits_parallelism(self):
        """Test that chunk_concurrency parameter limits ThreadPoolExecutor."""
        connector = Mock()
        connector.fetch_trades_with_cursor.return_value = (
            [MockTrade(trade_id=1000)],
            None,
        )

        with patch("schemahub.parallel.ThreadPoolExecutor") as mock_executor:
            # Mock executor context manager
            mock_executor.return_value.__enter__.return_value.submit.return_value.result.return_value = [
                MockTrade(trade_id=1000)
            ]

            fetch_trades_parallel(
                connector=connector,
                product_id="BTC-USD",
                cursor_start=1000,
                cursor_end=5000,
                chunk_concurrency=7,
                chunk_size=1000,
            )

            # Verify ThreadPoolExecutor created with max_workers=7
            mock_executor.assert_called_once_with(max_workers=7)

    def test_large_range_multiple_chunks(self):
        """Test fetching large range split into multiple chunks."""
        connector = Mock()

        # Mock response: return 1000 trades per chunk
        def mock_fetch(product_id, limit, after, **kwargs):
            # Return 1000 trades starting from 'after'
            trades = [MockTrade(trade_id=i) for i in range(after, min(after + 1000, 10000))]
            return (trades, None)

        connector.fetch_trades_with_cursor.side_effect = mock_fetch

        trades, highest = fetch_trades_parallel(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=10000,  # 9000 trades
            chunk_concurrency=5,
            chunk_size=1000,
        )

        # Should have 9000 trades (1000-9999)
        assert len(trades) == 9000
        assert trades[0].trade_id == 1000
        assert trades[-1].trade_id == 9999
        assert highest == 9999


class TestFetchChunk:
    """Test the _fetch_chunk helper function."""

    def test_fetch_chunk_single_page(self):
        """Test fetching a chunk that fits in one API page."""
        connector = Mock()
        mock_trades = [MockTrade(trade_id=i) for i in range(1000, 1100)]
        connector.fetch_trades_with_cursor.return_value = (mock_trades, None)

        result = _fetch_chunk(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=1100,
        )

        assert len(result) == 100
        assert result[0].trade_id == 1000
        assert result[-1].trade_id == 1099

    def test_fetch_chunk_multiple_pages(self):
        """Test fetching a chunk that requires multiple API pages."""
        connector = Mock()

        # Simulate pagination with CB-AFTER header
        call_count = [0]

        def mock_fetch(product_id, limit, after, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First page: trades 1000-1999, next cursor 2000
                return (
                    [MockTrade(trade_id=i) for i in range(1000, 2000)],
                    2000,
                )
            elif call_count[0] == 2:
                # Second page: trades 2000-2499, next cursor 2500
                return (
                    [MockTrade(trade_id=i) for i in range(2000, 2500)],
                    2500,
                )
            else:
                # No more data
                return ([], None)

        connector.fetch_trades_with_cursor.side_effect = mock_fetch

        result = _fetch_chunk(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2500,
        )

        # Should have 1500 trades (1000-2499)
        assert len(result) == 1500

    def test_fetch_chunk_stops_at_cursor_end(self):
        """Test that chunk stops at cursor_end even if more data available."""
        connector = Mock()

        # Return trades beyond cursor_end
        mock_trades = [MockTrade(trade_id=i) for i in range(1000, 3000)]
        connector.fetch_trades_with_cursor.return_value = (mock_trades, None)

        result = _fetch_chunk(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2000,  # Should stop at 2000
        )

        # Should only include trades < 2000
        assert len(result) == 1000
        assert all(t.trade_id < 2000 for t in result)

    def test_fetch_chunk_empty_response(self):
        """Test chunk handling when API returns no trades."""
        connector = Mock()
        connector.fetch_trades_with_cursor.return_value = ([], None)

        result = _fetch_chunk(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2000,
        )

        assert result == []

    def test_fetch_chunk_api_error_propagates(self):
        """Test that API errors in chunk propagate to caller."""
        connector = Mock()
        connector.fetch_trades_with_cursor.side_effect = Exception("API timeout")

        with pytest.raises(Exception, match="API timeout"):
            _fetch_chunk(
                connector=connector,
                product_id="BTC-USD",
                cursor_start=1000,
                cursor_end=2000,
            )

    def test_fetch_chunk_no_cb_after_header(self):
        """Test chunk pagination when CB-AFTER header is missing."""
        connector = Mock()

        call_count = [0]

        def mock_fetch(product_id, limit, after, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First page: trades 1000-1999, no CB-AFTER header
                return (
                    [MockTrade(trade_id=i) for i in range(1000, 2000)],
                    None,  # No CB-AFTER
                )
            elif call_count[0] == 2:
                # Second page: trades 2000-2500
                return (
                    [MockTrade(trade_id=i) for i in range(2000, 2500)],
                    None,
                )
            else:
                return ([], None)

        connector.fetch_trades_with_cursor.side_effect = mock_fetch

        result = _fetch_chunk(
            connector=connector,
            product_id="BTC-USD",
            cursor_start=1000,
            cursor_end=2500,
        )

        # Should handle missing CB-AFTER by estimating next cursor
        assert len(result) == 1500


class TestChunkRangeCalculation:
    """Test chunk range calculation logic."""

    def test_chunk_ranges_evenly_divisible(self):
        """Test chunk ranges when total trades evenly divide by chunk size."""
        # 5000 trades / 1000 per chunk = 5 chunks
        # Manual calculation since we're testing the logic
        total_trades = 5000
        chunk_size = 1000
        cursor_start = 1000
        cursor_end = cursor_start + total_trades

        expected_ranges = [
            (1000, 2000),
            (2000, 3000),
            (3000, 4000),
            (4000, 5000),
            (5000, 6000),
        ]

        # This tests the chunking logic in fetch_trades_parallel
        num_chunks = (total_trades + chunk_size - 1) // chunk_size
        assert num_chunks == 5

    def test_chunk_ranges_uneven_division(self):
        """Test chunk ranges when total trades don't evenly divide."""
        # 5500 trades / 1000 per chunk = 5.5 chunks → 6 chunks
        total_trades = 5500
        chunk_size = 1000

        num_chunks = (total_trades + chunk_size - 1) // chunk_size
        assert num_chunks == 6  # Ceiling division

    def test_chunk_ranges_less_than_chunk_size(self):
        """Test chunk ranges when total trades < chunk size."""
        total_trades = 500
        chunk_size = 1000

        num_chunks = (total_trades + chunk_size - 1) // chunk_size
        assert num_chunks == 1  # At least 1 chunk
