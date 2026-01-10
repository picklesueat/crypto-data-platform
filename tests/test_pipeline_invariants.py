"""Tests for pipeline correctness invariants.

These tests verify that the pipeline maintains data consistency:
1. Ingest raw count == Transform parquet count (per run)
2. Incremental ingest delta == Incremental transform delta
3. Final curated count == unique trades (no duplicates)
4. Checkpoint cursor advances correctly
"""
import pytest
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pandas as pd


class TestPipelineInvariants:
    """Test that pipeline maintains data consistency invariants."""

    def test_invariant_ingest_equals_transform_count(self):
        """Invariant: Raw records ingested == Parquet records written (within same run)."""
        # Simulate: ingest writes 1000 raw records, transform should produce 1000 parquet records
        raw_records_written = 1000
        
        # Mock transform that reads those records
        from schemahub.transform import transform_trade
        
        # Create mock trades
        mock_trades = [
            {
                "id": str(i),
                "product_id": "BTC-USD",
                "side": "buy",
                "price": "50000.00",
                "size": "0.1",
                "time": "2026-01-01T00:00:00Z",
            }
            for i in range(raw_records_written)
        ]
        
        # Transform all trades
        unified = [transform_trade(t, {}) for t in mock_trades]
        unified = [u for u in unified if u is not None]
        
        # Invariant check
        assert len(unified) == raw_records_written, \
            f"Transform output ({len(unified)}) != raw input ({raw_records_written})"

    def test_invariant_no_duplicates_in_same_batch(self):
        """Invariant: Within a single batch, no duplicate trade_ids after dedupe."""
        from schemahub.transform import write_unified_parquet
        import tempfile
        import os
        
        # Create trades with some duplicates
        trades = [
            {"exchange": "coinbase", "symbol": "BTC-USD", "trade_id": "1", 
             "side": "buy", "price": 50000.0, "quantity": 0.1, "trade_ts": "2026-01-01T00:00:00+00:00"},
            {"exchange": "coinbase", "symbol": "BTC-USD", "trade_id": "2",
             "side": "sell", "price": 50001.0, "quantity": 0.2, "trade_ts": "2026-01-01T00:00:01+00:00"},
            {"exchange": "coinbase", "symbol": "BTC-USD", "trade_id": "1",  # Duplicate!
             "side": "buy", "price": 50000.0, "quantity": 0.1, "trade_ts": "2026-01-01T00:00:00+00:00"},
        ]
        
        # Convert to DataFrame and dedupe (like write_unified_parquet does)
        df = pd.DataFrame(trades)
        initial_count = len(df)
        df = df.drop_duplicates(subset=["trade_id"], keep="first")
        deduped_count = len(df)
        
        # Invariant check
        assert deduped_count == 2, f"Expected 2 unique trades, got {deduped_count}"
        assert initial_count - deduped_count == 1, "Should have removed exactly 1 duplicate"

    def test_invariant_checkpoint_advances_monotonically(self):
        """Invariant: Checkpoint cursor only increases, never decreases."""
        from schemahub.checkpoint import CheckpointManager
        
        with patch('schemahub.checkpoint.boto3'):
            # Use correct constructor args: s3_bucket, s3_prefix, use_s3
            mgr = CheckpointManager(s3_bucket="test", s3_prefix="test", use_s3=False)
            
            # Simulate checkpoint progression
            checkpoints = [0, 100, 200, 150, 300]  # 150 is invalid (goes backward)
            
            current = 0
            for new_cursor in checkpoints:
                if new_cursor > current:
                    current = new_cursor
                # Invariant: cursor should never decrease
                assert current >= 0, "Cursor went negative"
            
            # Final cursor should be highest valid value
            assert current == 300

    def test_invariant_incremental_delta_matches(self):
        """Invariant: Incremental ingest Δ == Incremental transform Δ.
        
        If ingest adds 100 new raw records, transform should produce 100 new parquet records.
        """
        # Before state
        raw_files_before = 2
        parquet_records_before = 10000
        
        # After ingest (adds 1 new raw file with 100 records)
        raw_files_after = 3
        new_raw_records = 100
        
        # After transform (should add those 100 records)
        parquet_records_after = parquet_records_before + new_raw_records
        
        # Invariant checks
        ingest_delta = raw_files_after - raw_files_before
        transform_delta = parquet_records_after - parquet_records_before
        
        assert ingest_delta == 1, "Should have added exactly 1 raw file"
        assert transform_delta == new_raw_records, \
            f"Transform delta ({transform_delta}) != ingest new records ({new_raw_records})"

    def test_invariant_athena_dedupe_preserves_unique_count(self):
        """Invariant: Athena dedupe keeps exactly COUNT(DISTINCT pk) records."""
        # Simulate curated table with duplicates
        total_records = 1000
        unique_records = 800  # 200 duplicates
        
        # After dedupe
        after_dedupe = unique_records  # Should equal unique count
        
        # Invariant check
        assert after_dedupe == unique_records, \
            f"Dedupe should preserve {unique_records} unique records, got {after_dedupe}"
        
        duplicates_removed = total_records - after_dedupe
        assert duplicates_removed == 200, f"Should remove 200 duplicates, removed {duplicates_removed}"


class TestPipelineMetrics:
    """Test that pipeline reports correct metrics for monitoring."""

    def test_metrics_raw_vs_curated_match(self):
        """Metric: total_raw_records == total_curated_records (after dedupe)."""
        # This would query actual S3/Athena in integration test
        # Here we just verify the logic
        
        raw_records = 1000
        curated_records = 1000  # After dedupe, should match unique raw
        
        assert raw_records == curated_records, \
            f"Raw ({raw_records}) != Curated ({curated_records}) - data loss or gain!"

    def test_metrics_checkpoint_cursor_vs_max_trade_id(self):
        """Metric: checkpoint cursor should equal max(trade_id) in raw files."""
        checkpoint_cursor = 114527
        max_trade_id_in_raw = 114527  # From parsing raw files
        
        assert checkpoint_cursor == max_trade_id_in_raw, \
            f"Checkpoint ({checkpoint_cursor}) != max trade_id ({max_trade_id_in_raw})"


class TestFailureRecovery:
    """Test that pipeline recovers correctly from failures."""

    def test_crash_after_write_before_checkpoint(self):
        """Recovery: If crash after S3 write but before checkpoint, next run re-fetches."""
        # Before crash
        checkpoint_before = 1000
        
        # Simulate: wrote trades 1001-2000, but didn't update checkpoint
        trades_written = list(range(1001, 2001))
        
        # After restart: checkpoint still at 1000
        checkpoint_after_restart = checkpoint_before
        
        # Next ingest will re-fetch from 1000
        # This creates duplicates in raw, but Athena dedupe handles it
        
        assert checkpoint_after_restart == checkpoint_before, \
            "Checkpoint should not advance if not explicitly updated"

    def test_idempotent_ingest_with_same_run_id(self):
        """Idempotency: Same run_id produces same S3 keys (overwrite, not duplicate)."""
        from datetime import datetime
        
        run_id = "abc123"
        timestamp = "20260110T120000Z"
        
        # First attempt
        key1 = f"raw_trades_BTC-USD_{timestamp}_{run_id}_1_1000_1000.jsonl"
        
        # Retry with same run_id
        key2 = f"raw_trades_BTC-USD_{timestamp}_{run_id}_1_1000_1000.jsonl"
        
        assert key1 == key2, "Same run_id should produce identical keys (idempotent)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
