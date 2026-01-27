"""Parallel trade fetching using shared work queue.

This module implements within-product parallelism by having multiple worker
threads pull cursor targets from a shared queue. Each worker fetches ONE page
per cursor target, exactly like sequential mode but concurrent.

Architecture:
    - Pre-calculate cursor targets: [cursor_start, cursor_start+limit, ...]
    - Put all targets in thread-safe queue.Queue()
    - N worker threads pop from queue, fetch, append results
    - Sort results by trade_id at end

This approach reuses the exact same API call pattern as sequential mode,
avoiding any confusion about Coinbase's cursor semantics.
"""
from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING, List, Tuple

from schemahub.config import DEFAULT_CHUNK_CONCURRENCY

if TYPE_CHECKING:
    from schemahub.connectors.coinbase import CoinbaseConnector, CoinbaseTrade

logger = logging.getLogger(__name__)


def fetch_trades_parallel(
    connector: CoinbaseConnector,
    product_id: str,
    cursor_start: int,
    cursor_end: int,
    chunk_concurrency: int = DEFAULT_CHUNK_CONCURRENCY,
    limit: int = 1000,
) -> Tuple[List[CoinbaseTrade], int]:
    """Fetch trades in parallel using a shared work queue.

    Uses the SAME cursor logic as sequential mode: each API call uses
    after=cursor_target where cursor_target increments by `limit` each time.
    Multiple threads pull cursor targets from a shared queue concurrently.

    Args:
        connector: CoinbaseConnector instance (thread-safe)
        product_id: Product to fetch (e.g., "BTC-USD")
        cursor_start: Starting cursor (after param for first fetch)
        cursor_end: Target cursor (max trade_id we want to reach)
        chunk_concurrency: Number of parallel worker threads
        limit: Trades per API request (default: 1000)

    Returns:
        Tuple of (all_trades sorted by trade_id, highest_trade_id)

    Raises:
        Exception: If any fetch fails (all retries handled in fetch_trades_with_cursor)
    """
    # Pre-calculate cursor targets (same math as sequential mode)
    # Sequential does: cursor = cursor_start, then cursor += limit each iteration
    # We pre-compute all these cursor values upfront
    cursor_targets = []
    cursor = cursor_start
    while cursor < cursor_end:
        cursor_targets.append(cursor)
        cursor += limit

    num_pages = len(cursor_targets)

    if num_pages == 0:
        logger.info(f"[PARALLEL] {product_id}: No pages to fetch (cursor_start={cursor_start} >= cursor_end={cursor_end})")
        return [], cursor_start

    logger.info(
        f"[PARALLEL] {product_id}: Fetching {num_pages} pages "
        f"with {chunk_concurrency} workers, cursor range [{cursor_start:,}, {cursor_end:,})"
    )

    # Shared work queue - threads pop cursor targets from here
    # queue.Queue is thread-safe, no duplicates possible
    work_queue: queue.Queue[int] = queue.Queue()
    for cursor_target in cursor_targets:
        work_queue.put(cursor_target)

    # Thread-safe results collection
    results_lock = threading.Lock()
    all_trades: List[CoinbaseTrade] = []
    errors: List[Tuple[int, str]] = []
    highest_trade_id = cursor_start
    pages_completed = 0

    def worker():
        """Worker thread: pulls cursor targets from queue, fetches trades.

        All retries are handled inside fetch_trades_with_cursor (15 attempts with 2s backoff).
        If a fetch fails after all retries, it's recorded as a permanent failure.
        """
        nonlocal highest_trade_id, pages_completed

        while True:
            try:
                cursor_target = work_queue.get_nowait()
            except queue.Empty:
                return  # No more work

            try:
                # Fetch ONE page - all retries handled inside fetch_trades_with_cursor
                trades, _ = connector.fetch_trades_with_cursor(
                    product_id=product_id,
                    limit=limit,
                    after=cursor_target,
                )

                if trades:
                    with results_lock:
                        all_trades.extend(trades)
                        batch_highest = max(t.trade_id for t in trades)
                        highest_trade_id = max(highest_trade_id, batch_highest)
                        pages_completed += 1

                    logger.debug(
                        f"[PARALLEL] {product_id}: cursor={cursor_target:,} "
                        f"fetched {len(trades)} trades (page {pages_completed}/{num_pages})"
                    )
                else:
                    with results_lock:
                        pages_completed += 1
                    logger.debug(
                        f"[PARALLEL] {product_id}: cursor={cursor_target:,} "
                        f"returned 0 trades"
                    )

            except Exception as e:
                # Permanent failure - all retries exhausted in fetch_trades_with_cursor
                with results_lock:
                    errors.append((cursor_target, str(e)))
                logger.error(
                    f"[PARALLEL] {product_id}: cursor={cursor_target:,} FAILED: {e}"
                )

            finally:
                work_queue.task_done()

    # Spawn worker threads (no more than number of pages)
    num_workers = min(chunk_concurrency, num_pages)
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    # Wait for all work to complete
    for t in threads:
        t.join()

    # Check for errors (all retries already exhausted in fetch_trades_with_cursor)
    if errors:
        error_msg = (
            f"[PARALLEL] {product_id}: {len(errors)} of {num_pages} fetches failed. "
            f"First error: cursor={errors[0][0]}, {errors[0][1]}"
        )
        logger.error(error_msg)
        raise Exception(error_msg)

    # Sort trades by trade_id (threads may complete out of order)
    # Critical for checkpoint integrity
    all_trades.sort(key=lambda t: t.trade_id)

    logger.info(
        f"[PARALLEL] {product_id}: Fetched {len(all_trades):,} trades in {num_pages} pages, "
        f"highest_trade_id={highest_trade_id:,}"
    )

    return all_trades, highest_trade_id


__all__ = ["fetch_trades_parallel"]
