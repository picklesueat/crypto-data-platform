"""Unit tests for data quality validation functions."""
import io
import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

from schemahub.validation import (
    validate_batch_and_check_manifest,
    validate_full_dataset_daily,
    check_data_quality_gates,
)


def create_parquet_bytes(df: pd.DataFrame) -> io.BytesIO:
    """Convert DataFrame to Parquet bytes buffer."""
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def create_valid_trades_df(num_records: int = 100, product: str = "BTC-USD") -> pd.DataFrame:
    """Create a valid trades DataFrame for testing."""
    now = datetime.now(timezone.utc)
    return pd.DataFrame({
        "exchange": ["coinbase"] * num_records,
        "symbol": [product] * num_records,
        "trade_id": list(range(1000, 1000 + num_records)),
        "side": ["buy", "sell"] * (num_records // 2),
        "price": [50000.0 + i for i in range(num_records)],
        "quantity": [0.1 + i * 0.01 for i in range(num_records)],
        "trade_ts": [now - timedelta(minutes=num_records - i) for i in range(num_records)],
        "ingest_ts": [now] * num_records,
    })


@pytest.fixture
def s3_bucket():
    """Create a mocked S3 bucket for testing."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)
        yield s3, bucket


# ============================================================================
# validate_batch_and_check_manifest tests
# ============================================================================

class TestValidateBatchAndCheckManifest:
    """Tests for validate_batch_and_check_manifest function."""

    def test_valid_parquet_file_passes_validation(self, s3_bucket):
        """Test validation passes for a valid Parquet file."""
        s3, bucket = s3_bucket

        # Create valid DataFrame
        df = create_valid_trades_df(100)
        parquet_bytes = create_parquet_bytes(df)

        # Upload to S3
        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
        )

        assert issues == []
        assert metrics["batch_records_checked"] == 100
        assert metrics["duplicates_found"] == 0
        assert metrics["schema_errors"] == 0

    def test_missing_required_columns(self, s3_bucket):
        """Test validation detects missing required columns."""
        s3, bucket = s3_bucket

        # Create DataFrame missing 'side' column
        df = pd.DataFrame({
            "exchange": ["coinbase"] * 10,
            "symbol": ["BTC-USD"] * 10,
            "trade_id": list(range(10)),
            "price": [50000.0] * 10,
            "quantity": [0.1] * 10,
            "trade_ts": [datetime.now(timezone.utc)] * 10,
            "ingest_ts": [datetime.now(timezone.utc)] * 10,
            # Missing: "side"
        })
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
        )

        assert any("Missing required columns" in issue for issue in issues)
        assert metrics["schema_errors"] == 1

    def test_detects_duplicate_trade_ids(self, s3_bucket):
        """Test validation detects duplicate trade_ids within batch."""
        s3, bucket = s3_bucket
        now = datetime.now(timezone.utc)

        # Create DataFrame with duplicates
        df = pd.DataFrame({
            "exchange": ["coinbase"] * 10,
            "symbol": ["BTC-USD"] * 10,
            "trade_id": [1, 2, 3, 3, 4, 5, 5, 6, 7, 8],  # Duplicates: 3 and 5
            "side": ["buy", "sell"] * 5,
            "price": [50000.0] * 10,
            "quantity": [0.1] * 10,
            "trade_ts": [now] * 10,
            "ingest_ts": [now] * 10,
        })
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
        )

        assert any("duplicate" in issue.lower() for issue in issues)
        assert metrics["duplicates_found"] == 2  # 2 duplicate pairs

    def test_detects_negative_price(self, s3_bucket):
        """Test validation detects negative/zero prices."""
        s3, bucket = s3_bucket
        now = datetime.now(timezone.utc)

        df = pd.DataFrame({
            "exchange": ["coinbase"] * 10,
            "symbol": ["BTC-USD"] * 10,
            "trade_id": list(range(10)),
            "side": ["buy", "sell"] * 5,
            "price": [50000.0, -100.0, 0.0, 50000.0, 50000.0, 50000.0, 50000.0, 50000.0, 50000.0, 50000.0],
            "quantity": [0.1] * 10,
            "trade_ts": [now] * 10,
            "ingest_ts": [now] * 10,
        })
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
        )

        assert any("invalid price" in issue.lower() for issue in issues)

    def test_detects_invalid_side_values(self, s3_bucket):
        """Test validation detects invalid side values."""
        s3, bucket = s3_bucket
        now = datetime.now(timezone.utc)

        df = pd.DataFrame({
            "exchange": ["coinbase"] * 10,
            "symbol": ["BTC-USD"] * 10,
            "trade_id": list(range(10)),
            "side": ["buy", "sell", "invalid", "buy", "sell", "buy", "sell", "buy", "sell", "buy"],
            "price": [50000.0] * 10,
            "quantity": [0.1] * 10,
            "trade_ts": [now] * 10,
            "ingest_ts": [now] * 10,
        })
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
        )

        assert any("invalid side" in issue.lower() for issue in issues)

    def test_detects_stale_products_from_manifest(self, s3_bucket):
        """Test validation detects stale products from manifest data."""
        s3, bucket = s3_bucket

        df = create_valid_trades_df(10)
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        # Create manifest with stale product
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
        manifest_data = {
            "product_stats": {
                "BTC-USD": {"last_update_ts": stale_time},
            }
        }

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key=key,
            manifest_data=manifest_data,
        )

        assert "BTC-USD" in metrics["stale_products"]

    def test_no_latest_key_returns_issue(self, s3_bucket):
        """Test validation handles missing latest_s3_key."""
        s3, bucket = s3_bucket

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key="",
        )

        assert any("No latest Parquet file" in issue for issue in issues)

    def test_handles_missing_s3_key_gracefully(self, s3_bucket):
        """Test validation handles non-existent S3 key gracefully."""
        s3, bucket = s3_bucket

        issues, metrics = validate_batch_and_check_manifest(
            bucket=bucket,
            unified_prefix="unified/v1",
            latest_s3_key="nonexistent/file.parquet",
        )

        assert any("Validation error" in issue for issue in issues)


# ============================================================================
# validate_full_dataset_daily tests
# ============================================================================

class TestValidateFullDatasetDaily:
    """Tests for validate_full_dataset_daily function."""

    def test_valid_dataset_passes_validation(self, s3_bucket):
        """Test validation passes for a valid full dataset."""
        s3, bucket = s3_bucket

        # Create and upload valid data
        df = create_valid_trades_df(100)
        parquet_bytes = create_parquet_bytes(df)

        key = "unified/v1/BTC-USD/trades.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_bytes.getvalue())

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        assert metrics["total_records"] == 100
        assert metrics["duplicates_found"] == 0
        assert "BTC-USD" in metrics["products"]

    def test_detects_duplicates_across_files(self, s3_bucket):
        """Test validation detects duplicates across multiple Parquet files."""
        s3, bucket = s3_bucket
        now = datetime.now(timezone.utc)

        # File 1 with trade_ids 1-50
        df1 = create_valid_trades_df(50, "BTC-USD")

        # File 2 with overlapping trade_ids (duplicate 1000-1010)
        df2 = create_valid_trades_df(50, "BTC-USD")  # Starts at trade_id 1000

        parquet1 = create_parquet_bytes(df1)
        parquet2 = create_parquet_bytes(df2)

        s3.put_object(Bucket=bucket, Key="unified/v1/BTC-USD/file1.parquet", Body=parquet1.getvalue())
        s3.put_object(Bucket=bucket, Key="unified/v1/BTC-USD/file2.parquet", Body=parquet2.getvalue())

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        # All 50 trades are duplicated
        assert metrics["duplicates_found"] == 50
        assert any("duplicate" in issue.lower() for issue in issues)

    def test_detects_stale_data(self, s3_bucket):
        """Test validation detects stale data (>1 hour old)."""
        s3, bucket = s3_bucket

        # Create data that's 2 hours old
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        df = pd.DataFrame({
            "exchange": ["coinbase"] * 10,
            "symbol": ["BTC-USD"] * 10,
            "trade_id": list(range(10)),
            "side": ["buy", "sell"] * 5,
            "price": [50000.0] * 10,
            "quantity": [0.1] * 10,
            "trade_ts": [old_time] * 10,
            "ingest_ts": [datetime.now(timezone.utc)] * 10,
        })
        parquet_bytes = create_parquet_bytes(df)

        s3.put_object(Bucket=bucket, Key="unified/v1/BTC-USD/trades.parquet", Body=parquet_bytes.getvalue())

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        assert metrics["date_range"]["age_hours"] > 1
        assert any("old" in issue.lower() or "hours" in issue.lower() for issue in issues)

    def test_no_parquet_files_returns_issue(self, s3_bucket):
        """Test validation handles no Parquet files found."""
        s3, bucket = s3_bucket

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        assert any("No Parquet files found" in issue for issue in issues)
        assert metrics["total_records"] == 0

    def test_multiple_products_coverage(self, s3_bucket):
        """Test validation tracks product coverage."""
        s3, bucket = s3_bucket

        # Create data for multiple products
        df1 = create_valid_trades_df(50, "BTC-USD")
        df2 = create_valid_trades_df(50, "ETH-USD")
        df2["trade_id"] = list(range(2000, 2050))  # Avoid duplicates

        parquet1 = create_parquet_bytes(df1)
        parquet2 = create_parquet_bytes(df2)

        s3.put_object(Bucket=bucket, Key="unified/v1/BTC-USD/trades.parquet", Body=parquet1.getvalue())
        s3.put_object(Bucket=bucket, Key="unified/v1/ETH-USD/trades.parquet", Body=parquet2.getvalue())

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        assert "BTC-USD" in metrics["products"]
        assert "ETH-USD" in metrics["products"]
        assert metrics["total_records"] == 100

    def test_detects_time_series_gaps(self, s3_bucket):
        """Test validation detects significant time series gaps."""
        s3, bucket = s3_bucket
        now = datetime.now(timezone.utc)

        # Create data with a 3-hour gap
        times = [
            now - timedelta(hours=5),  # Old trade
            now - timedelta(hours=4),
            now - timedelta(hours=3),
            # GAP of 2.5 hours
            now - timedelta(minutes=30),
            now - timedelta(minutes=20),
            now - timedelta(minutes=10),
            now,
        ]

        df = pd.DataFrame({
            "exchange": ["coinbase"] * len(times),
            "symbol": ["BTC-USD"] * len(times),
            "trade_id": list(range(len(times))),
            "side": ["buy"] * len(times),
            "price": [50000.0] * len(times),
            "quantity": [0.1] * len(times),
            "trade_ts": times,
            "ingest_ts": [now] * len(times),
        })
        parquet_bytes = create_parquet_bytes(df)

        s3.put_object(Bucket=bucket, Key="unified/v1/BTC-USD/trades.parquet", Body=parquet_bytes.getvalue())

        issues, metrics = validate_full_dataset_daily(
            bucket=bucket,
            unified_prefix="unified/v1",
        )

        # Should detect the gap
        assert "gap_issues" in metrics or metrics.get("max_gap_minutes", 0) > 60


# ============================================================================
# check_data_quality_gates tests
# ============================================================================

class TestCheckDataQualityGates:
    """Tests for check_data_quality_gates function."""

    def test_passes_with_no_issues(self):
        """Test gates pass when no issues."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 0,
            "schema_errors": 0,
        }

        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        assert passes is True
        assert reasons == []

    def test_fails_on_missing_columns(self):
        """Test gates fail on missing required columns."""
        batch_issues = ["Missing required columns: {'side'}"]
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 0,
            "schema_errors": 1,
        }

        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        assert passes is False
        assert any("Missing required columns" in reason for reason in reasons)

    def test_fails_on_high_duplicate_percentage(self):
        """Test gates fail when duplicate percentage > 5%."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 10,  # 10% duplicates
            "schema_errors": 0,
        }

        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        assert passes is False
        assert any("DUPLICATES" in reason for reason in reasons)

    def test_passes_on_low_duplicate_percentage(self):
        """Test gates pass when duplicate percentage <= 5%."""
        batch_issues = ["Found 5 duplicate trade_ids in batch"]
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 5,  # Exactly 5% duplicates - should pass
            "schema_errors": 0,
        }

        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        assert passes is True

    def test_fails_on_validation_error(self):
        """Test gates fail on validation errors."""
        batch_issues = ["Validation error: Connection timeout"]
        batch_metrics = {
            "batch_records_checked": 0,
            "duplicates_found": 0,
            "schema_errors": 0,
        }

        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        assert passes is False
        assert any("Validation error" in reason for reason in reasons)

    def test_fails_on_stale_data_full_validation(self):
        """Test gates fail when full validation shows data > 4 hours old."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 0,
        }
        full_issues = []
        full_metrics = {
            "total_records": 1000,
            "date_range": {"age_hours": 5.0},  # 5 hours old
        }

        passes, reasons = check_data_quality_gates(
            batch_issues, batch_metrics, full_issues, full_metrics
        )

        assert passes is False
        assert any("FRESHNESS" in reason for reason in reasons)

    def test_passes_on_fresh_data(self):
        """Test gates pass when data is fresh (< 4 hours)."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 0,
        }
        full_issues = []
        full_metrics = {
            "total_records": 1000,
            "date_range": {"age_hours": 2.0},  # 2 hours old - within threshold
        }

        passes, reasons = check_data_quality_gates(
            batch_issues, batch_metrics, full_issues, full_metrics
        )

        assert passes is True

    def test_multiple_failures(self):
        """Test multiple gate failures are all reported."""
        batch_issues = ["Missing required columns: {'side'}"]
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 20,  # 20% duplicates
            "schema_errors": 1,
        }
        full_issues = []
        full_metrics = {
            "total_records": 1000,
            "date_range": {"age_hours": 6.0},  # 6 hours old
        }

        passes, reasons = check_data_quality_gates(
            batch_issues, batch_metrics, full_issues, full_metrics
        )

        assert passes is False
        assert len(reasons) >= 3  # Schema, duplicates, freshness

    def test_handles_none_full_validation(self):
        """Test gates work when full validation is not provided."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 100,
            "duplicates_found": 0,
        }

        passes, reasons = check_data_quality_gates(
            batch_issues, batch_metrics, full_issues=None, full_metrics=None
        )

        assert passes is True

    def test_handles_zero_batch_size(self):
        """Test gates handle zero batch size without division error."""
        batch_issues = []
        batch_metrics = {
            "batch_records_checked": 0,
            "duplicates_found": 5,  # Duplicates but no batch size
        }

        # Should not raise division by zero
        passes, reasons = check_data_quality_gates(batch_issues, batch_metrics)

        # Passes because we can't calculate percentage with 0 batch size
        assert passes is True
