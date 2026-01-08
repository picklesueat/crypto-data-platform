"""Unit tests for CheckpointManager."""
import json
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.stub import Stubber

from schemahub.checkpoint import CheckpointManager


class TestCheckpointManagerLocal:
    """Tests for local filesystem checkpointing."""

    def test_load_nonexistent_checkpoint_returns_empty_dict(self):
        """Loading a checkpoint that doesn't exist returns an empty dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            result = mgr.load("BTC-USD")
            
            assert result == {}

    def test_save_and_load_checkpoint_locally(self):
        """Saving and loading a checkpoint locally works correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            checkpoint = {"last_trade_id": 12345, "custom_field": "value"}
            mgr.save("BTC-USD", checkpoint)
            
            loaded = mgr.load("BTC-USD")
            
            assert loaded["last_trade_id"] == 12345
            assert loaded["custom_field"] == "value"
            assert "last_updated" in loaded

    def test_save_adds_timestamp(self):
        """Saving a checkpoint adds a last_updated timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            checkpoint = {"last_trade_id": 12345}
            before_save = datetime.utcnow().isoformat()
            mgr.save("BTC-USD", checkpoint)
            after_save = datetime.utcnow().isoformat()
            
            loaded = mgr.load("BTC-USD")
            
            assert "last_updated" in loaded
            # Verify timestamp is between before and after (with some tolerance)
            assert before_save < loaded["last_updated"] < after_save + "9"

    def test_save_uses_atomic_write(self):
        """Saving a checkpoint uses atomic writes (tmp then rename)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            with patch("os.replace") as mock_replace:
                checkpoint = {"last_trade_id": 12345}
                mgr.save("BTC-USD", checkpoint)
                
                # Verify os.replace was called (atomic rename)
                assert mock_replace.called
                args = mock_replace.call_args[0]
                assert args[0].endswith(".tmp")
                assert args[1].endswith("BTC-USD.json")

    def test_load_corrupted_json_returns_empty_dict(self):
        """Loading a corrupted JSON file returns an empty dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            # Write invalid JSON
            path = os.path.join(tmpdir, "BTC-USD.json")
            with open(path, "w") as f:
                f.write("{invalid json")
            
            result = mgr.load("BTC-USD")
            
            assert result == {}

    def test_local_path_format(self):
        """_local_path returns correctly formatted path with mode subdirectory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False, mode="ingest")
            mgr.local_dir = tmpdir
            
            path = mgr._local_path("BTC-USD")
            
            assert path == os.path.join(tmpdir, "ingest", "BTC-USD.json")

    def test_multiple_products_isolated(self):
        """Checkpoints for different products are isolated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(s3_bucket="unused", s3_prefix="unused", use_s3=False)
            mgr.local_dir = tmpdir
            
            mgr.save("BTC-USD", {"last_trade_id": 100})
            mgr.save("ETH-USD", {"last_trade_id": 200})
            
            btc = mgr.load("BTC-USD")
            eth = mgr.load("ETH-USD")
            
            assert btc["last_trade_id"] == 100
            assert eth["last_trade_id"] == 200


class TestCheckpointManagerS3:
    """Tests for S3 checkpointing."""

    def test_s3_key_format(self):
        """_s3_key returns correctly formatted S3 key with mode subdirectory."""
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data/trades", use_s3=False, mode="ingest")
        
        key = mgr._s3_key("BTC-USD")
        
        assert key == "data/trades/checkpoints/ingest/BTC-USD.json"

    def test_s3_key_handles_trailing_slash_in_prefix(self):
        """_s3_key strips trailing slash from prefix."""
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data/trades/", use_s3=False, mode="ingest")
        
        key = mgr._s3_key("BTC-USD")
        
        assert key == "data/trades/checkpoints/ingest/BTC-USD.json"

    def test_save_to_s3(self):
        """Saving a checkpoint to S3 works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data", use_s3=True)
        mgr.s3 = client
        
        checkpoint = {"last_trade_id": 12345}
        
        # Stub the put_object call
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should not raise any exception
            mgr.save("BTC-USD", checkpoint)

    def test_load_from_s3(self):
        """Loading a checkpoint from S3 works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data", use_s3=True, mode="ingest")
        mgr.s3 = client
        
        checkpoint_data = {"last_trade_id": 12345, "last_updated": "2025-12-15T10:00:00Z"}
        
        # Stub the get_object call
        stubber.add_response(
            "get_object",
            {"Body": MagicMock(read=lambda: json.dumps(checkpoint_data).encode())},
            {"Bucket": "my-bucket", "Key": "data/checkpoints/ingest/BTC-USD.json"},
        )
        
        with stubber:
            result = mgr.load("BTC-USD")
            
            assert result["last_trade_id"] == 12345
            assert result["last_updated"] == "2025-12-15T10:00:00Z"

    def test_load_nonexistent_s3_key_returns_empty_dict(self):
        """Loading a nonexistent S3 key returns an empty dict."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data", use_s3=True)
        mgr.s3 = client
        
        # Stub the get_object call to raise NoSuchKey
        stubber.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
        )
        
        with stubber:
            result = mgr.load("NONEXISTENT-USD")
            
            assert result == {}

    def test_load_s3_error_returns_empty_dict(self):
        """Loading from S3 with any error returns an empty dict."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data", use_s3=True)
        mgr.s3 = client
        
        # Stub the get_object call to raise a general error
        stubber.add_client_error(
            "get_object",
            service_error_code="AccessDenied",
            http_status_code=403,
        )
        
        with stubber:
            result = mgr.load("BTC-USD")
            
            assert result == {}

    def test_s3_checkpoint_includes_timestamp(self):
        """S3 checkpoints include last_updated timestamp."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        mgr = CheckpointManager(s3_bucket="my-bucket", s3_prefix="data", use_s3=True)
        mgr.s3 = client
        
        checkpoint = {"last_trade_id": 12345}
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Save should add timestamp to checkpoint dict
            mgr.save("BTC-USD", checkpoint)
            # Verify timestamp was added to the original checkpoint dict
            assert "last_updated" in checkpoint
            assert checkpoint["last_updated"].endswith("Z")
