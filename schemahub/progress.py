"""Progress tracking for backfill operations.

Provides real-time progress bars showing:
- Percentage complete
- Records processed vs total
- Processing rate (records/minute)
- Estimated time remaining
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict

logger = logging.getLogger(__name__)


@dataclass
class ProductProgress:
    """Track progress for a single product."""
    product_id: str
    start_cursor: int
    target_trade_id: int
    records_processed: int = 0
    last_cursor: int = 0

    @property
    def total_records_expected(self) -> int:
        """Calculate total records expected based on trade ID range."""
        return max(0, self.target_trade_id - self.start_cursor)

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage (0-100)."""
        if self.total_records_expected == 0:
            return 100.0
        return min(100.0, (self.records_processed / self.total_records_expected) * 100)


class ProgressTracker:
    """Thread-safe progress tracker for backfill operations.

    Tracks progress across multiple products and provides periodic progress updates
    with rate calculations and progress bars.
    """

    def __init__(self, update_interval_seconds: int = 120):
        """Initialize progress tracker.

        Args:
            update_interval_seconds: Minimum seconds between progress updates (default 120 = 2 minutes)
        """
        self.update_interval = update_interval_seconds
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.products: Dict[str, ProductProgress] = {}
        self._lock = Lock()

        logger.info(f"ProgressTracker initialized (update interval: {update_interval_seconds}s)")

    def add_product(self, product_id: str, start_cursor: int, target_trade_id: int):
        """Register a product for progress tracking.

        Args:
            product_id: Coinbase product ID
            start_cursor: Starting cursor (trade ID)
            target_trade_id: Target (latest) trade ID
        """
        with self._lock:
            self.products[product_id] = ProductProgress(
                product_id=product_id,
                start_cursor=start_cursor,
                target_trade_id=target_trade_id,
            )
            logger.debug(f"Added product {product_id}: cursor={start_cursor}, target={target_trade_id}, expected_records={target_trade_id - start_cursor}")

    def update_progress(self, product_id: str, records_added: int, current_cursor: int):
        """Update progress for a product.

        Args:
            product_id: Product being updated
            records_added: Number of records just processed
            current_cursor: Current cursor position
        """
        with self._lock:
            if product_id not in self.products:
                logger.warning(f"Product {product_id} not registered in progress tracker")
                return

            product = self.products[product_id]
            product.records_processed += records_added
            product.last_cursor = current_cursor

    def should_print_update(self) -> bool:
        """Check if enough time has passed to print an update."""
        current_time = time.time()
        elapsed = current_time - self.last_update_time
        return elapsed >= self.update_interval

    def print_progress(self, force: bool = False):
        """Print progress update to stdout.

        Args:
            force: Force print even if update interval hasn't elapsed
        """
        if not force and not self.should_print_update():
            return

        with self._lock:
            current_time = time.time()
            elapsed_seconds = current_time - self.start_time
            elapsed_minutes = elapsed_seconds / 60.0

            # Calculate totals
            total_expected = sum(p.total_records_expected for p in self.products.values())
            total_processed = sum(p.records_processed for p in self.products.values())

            if total_expected == 0:
                logger.debug("No records to process, skipping progress update")
                return

            # Calculate rate
            records_per_minute = total_processed / elapsed_minutes if elapsed_minutes > 0 else 0

            # Calculate ETA
            remaining_records = total_expected - total_processed
            eta_minutes = remaining_records / records_per_minute if records_per_minute > 0 else 0

            # Overall progress percentage
            overall_percent = (total_processed / total_expected) * 100 if total_expected > 0 else 0

            # Build progress bar (50 chars wide)
            bar_width = 50
            filled = int(bar_width * overall_percent / 100)
            bar = '█' * filled + '░' * (bar_width - filled)

            # Print progress update
            print("\n" + "=" * 80)
            print(f"BACKFILL PROGRESS UPDATE - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print("=" * 80)
            print(f"[{bar}] {overall_percent:.1f}%")
            print(f"Records: {total_processed:,} / {total_expected:,}")
            print(f"Rate: {records_per_minute:,.0f} records/minute")
            print(f"Elapsed: {elapsed_minutes:.1f} minutes")
            if eta_minutes > 0 and eta_minutes < 10000:  # Only show reasonable ETAs
                print(f"ETA: {eta_minutes:.1f} minutes ({eta_minutes/60:.1f} hours)")
            print(f"Products: {len(self.products)} total")

            # Show per-product breakdown (top 10 by progress)
            sorted_products = sorted(
                self.products.values(),
                key=lambda p: p.progress_percent,
                reverse=False  # Show least progress first
            )

            if len(sorted_products) > 0:
                print("\nPer-Product Progress (showing up to 10):")
                for product in sorted_products[:10]:
                    if product.total_records_expected > 0:
                        mini_bar_width = 20
                        mini_filled = int(mini_bar_width * product.progress_percent / 100)
                        mini_bar = '█' * mini_filled + '░' * (mini_bar_width - mini_filled)
                        print(f"  {product.product_id:15} [{mini_bar}] {product.progress_percent:5.1f}% "
                              f"({product.records_processed:,} / {product.total_records_expected:,})")

            print("=" * 80 + "\n")

            # Update last update time
            self.last_update_time = current_time

    def print_final_summary(self):
        """Print final summary at completion."""
        with self._lock:
            elapsed_seconds = time.time() - self.start_time
            elapsed_minutes = elapsed_seconds / 60.0

            total_expected = sum(p.total_records_expected for p in self.products.values())
            total_processed = sum(p.records_processed for p in self.products.values())
            records_per_minute = total_processed / elapsed_minutes if elapsed_minutes > 0 else 0

            print("\n" + "=" * 80)
            print(f"BACKFILL COMPLETE - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print("=" * 80)
            print(f"Total Records Processed: {total_processed:,}")
            print(f"Total Expected: {total_expected:,}")
            print(f"Average Rate: {records_per_minute:,.0f} records/minute")
            print(f"Total Time: {elapsed_minutes:.1f} minutes ({elapsed_minutes/60:.2f} hours)")
            print(f"Products Processed: {len(self.products)}")

            # Show completion status
            completed = sum(1 for p in self.products.values() if p.records_processed >= p.total_records_expected)
            print(f"Completed Products: {completed} / {len(self.products)}")

            print("=" * 80 + "\n")
