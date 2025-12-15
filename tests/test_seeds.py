"""Unit tests for seed file management."""
import os
import tempfile
from unittest.mock import patch

import pytest
import yaml

from schemahub.connectors.coinbase import CoinbaseConnector


class TestLoadProductSeed:
    """Tests for loading product seed files."""

    def test_load_seed_from_yaml_file(self):
        """Loading a seed file returns product_ids and metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            seed_data = {
                "product_ids": ["BTC-USD", "ETH-USD"],
                "metadata": {"source": "manual", "updated": "2025-12-15"},
            }
            with open(seed_path, "w") as f:
                yaml.dump(seed_data, f)
            
            product_ids, metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert product_ids == ["BTC-USD", "ETH-USD"]
            assert metadata == {"source": "manual", "updated": "2025-12-15"}

    def test_load_seed_nonexistent_file_returns_empty(self):
        """Loading a nonexistent seed file returns empty list and metadata."""
        result_ids, result_meta = CoinbaseConnector.load_product_seed("/nonexistent/path.yaml")
        
        assert result_ids == []
        assert result_meta == {}

    def test_load_seed_empty_yaml_file(self):
        """Loading an empty YAML file returns empty defaults."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            with open(seed_path, "w") as f:
                f.write("")
            
            product_ids, metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert product_ids == []
            assert metadata == {}

    def test_load_seed_missing_product_ids_key(self):
        """Loading a seed file without product_ids key returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            seed_data = {"metadata": {"source": "manual"}}
            with open(seed_path, "w") as f:
                yaml.dump(seed_data, f)
            
            product_ids, metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert product_ids == []
            assert metadata == {"source": "manual"}

    def test_load_seed_missing_metadata_key(self):
        """Loading a seed file without metadata key returns empty metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            seed_data = {"product_ids": ["BTC-USD"]}
            with open(seed_path, "w") as f:
                yaml.dump(seed_data, f)
            
            product_ids, metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert product_ids == ["BTC-USD"]
            assert metadata == {}

    def test_load_seed_null_product_ids(self):
        """Loading a seed file with null product_ids returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            seed_data = {"product_ids": None, "metadata": {"source": "manual"}}
            with open(seed_path, "w") as f:
                yaml.dump(seed_data, f)
            
            product_ids, metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert product_ids == []
            assert metadata == {"source": "manual"}

    def test_load_seed_uses_default_path_when_none(self):
        """load_product_seed uses DEFAULT_SEED_PATH when path is None."""
        with patch("schemahub.connectors.coinbase.DEFAULT_SEED_PATH", "/nonexistent/seed.yaml"):
            product_ids, metadata = CoinbaseConnector.load_product_seed(None)
            
            # Should return empty because default path doesn't exist
            assert product_ids == []
            assert metadata == {}

    def test_load_seed_returns_tuple(self):
        """load_product_seed returns a tuple of (list, dict)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            seed_data = {
                "product_ids": ["BTC-USD"],
                "metadata": {"source": "test"},
            }
            with open(seed_path, "w") as f:
                yaml.dump(seed_data, f)
            
            result = CoinbaseConnector.load_product_seed(seed_path)
            
            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], list)
            assert isinstance(result[1], dict)


class TestSaveProductSeed:
    """Tests for saving product seed files."""

    def test_save_seed_creates_file(self):
        """Saving a seed file creates the file with correct content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            product_ids = ["BTC-USD", "ETH-USD"]
            metadata = {"source": "manual", "version": 1}
            
            CoinbaseConnector.save_product_seed(product_ids, seed_path, metadata)
            
            assert os.path.exists(seed_path)
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            assert saved_data["product_ids"] == ["BTC-USD", "ETH-USD"]
            assert saved_data["metadata"]["source"] == "manual"

    def test_save_seed_creates_parent_directories(self):
        """Saving a seed file creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "subdir1", "subdir2", "seed.yaml")
            
            CoinbaseConnector.save_product_seed(["BTC-USD"], seed_path)
            
            assert os.path.exists(seed_path)

    def test_save_seed_adds_last_updated_timestamp(self):
        """Saving a seed file adds a last_updated timestamp to metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            product_ids = ["BTC-USD"]
            
            CoinbaseConnector.save_product_seed(product_ids, seed_path)
            
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            
            assert "last_updated" in saved_data["metadata"]
            # Verify it ends with 'Z' (ISO format with Z suffix)
            assert saved_data["metadata"]["last_updated"].endswith("Z")

    def test_save_seed_preserves_existing_metadata(self):
        """Saving a seed file preserves existing metadata and adds timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            product_ids = ["BTC-USD"]
            metadata = {"source": "manual", "custom": "value"}
            
            CoinbaseConnector.save_product_seed(product_ids, seed_path, metadata)
            
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            
            assert saved_data["metadata"]["source"] == "manual"
            assert saved_data["metadata"]["custom"] == "value"
            assert "last_updated" in saved_data["metadata"]

    def test_save_seed_converts_iterable_to_list(self):
        """Saving a seed file converts iterable to list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            product_ids = {"BTC-USD", "ETH-USD"}  # Set, not list
            
            CoinbaseConnector.save_product_seed(product_ids, seed_path)
            
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            
            assert isinstance(saved_data["product_ids"], list)
            assert set(saved_data["product_ids"]) == {"BTC-USD", "ETH-USD"}

    def test_save_seed_uses_atomic_write(self):
        """Saving a seed file uses atomic write (tmp then rename)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            
            with patch("os.replace") as mock_replace:
                CoinbaseConnector.save_product_seed(["BTC-USD"], seed_path)
                
                # Verify os.replace was called
                assert mock_replace.called
                args = mock_replace.call_args[0]
                assert args[0].endswith(".tmp")
                assert args[1] == seed_path

    def test_save_seed_none_metadata_uses_empty_dict(self):
        """Saving a seed file with None metadata uses an empty dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            
            CoinbaseConnector.save_product_seed(["BTC-USD"], seed_path, None)
            
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            
            assert isinstance(saved_data["metadata"], dict)
            assert "last_updated" in saved_data["metadata"]

    def test_save_seed_overwrites_existing_file(self):
        """Saving a seed file overwrites an existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            
            # Write initial data
            initial = {"product_ids": ["BTC-USD"], "metadata": {"v": 1}}
            with open(seed_path, "w") as f:
                yaml.dump(initial, f)
            
            # Overwrite with new data
            CoinbaseConnector.save_product_seed(["ETH-USD", "DOGE-USD"], seed_path, {"v": 2})
            
            with open(seed_path, "r") as f:
                saved_data = yaml.safe_load(f)
            
            assert saved_data["product_ids"] == ["ETH-USD", "DOGE-USD"]
            assert saved_data["metadata"]["v"] == 2

    def test_save_seed_uses_default_path_when_none(self):
        """save_product_seed uses DEFAULT_SEED_PATH when path is None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            default_path = os.path.join(tmpdir, "default_seed.yaml")
            with patch("schemahub.connectors.coinbase.DEFAULT_SEED_PATH", default_path):
                CoinbaseConnector.save_product_seed(["BTC-USD"])
                
                assert os.path.exists(default_path)


class TestRoundTripSeedOperations:
    """Tests for round-trip save and load operations."""

    def test_save_and_load_round_trip(self):
        """Saving and loading a seed file preserves data (except timestamp)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            original_ids = ["BTC-USD", "ETH-USD", "DOGE-USD"]
            original_metadata = {"source": "coinbase-api", "version": 2}
            
            CoinbaseConnector.save_product_seed(original_ids, seed_path, original_metadata)
            loaded_ids, loaded_metadata = CoinbaseConnector.load_product_seed(seed_path)
            
            assert loaded_ids == original_ids
            assert loaded_metadata["source"] == original_metadata["source"]
            assert loaded_metadata["version"] == original_metadata["version"]
            assert "last_updated" in loaded_metadata

    def test_multiple_save_operations_preserve_custom_metadata(self):
        """Multiple saves preserve custom metadata while updating timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            seed_path = os.path.join(tmpdir, "seed.yaml")
            
            # First save
            CoinbaseConnector.save_product_seed(["BTC-USD"], seed_path, {"custom": "data"})
            _, meta1 = CoinbaseConnector.load_product_seed(seed_path)
            
            # Second save with same metadata
            CoinbaseConnector.save_product_seed(["BTC-USD", "ETH-USD"], seed_path, {"custom": "data"})
            _, meta2 = CoinbaseConnector.load_product_seed(seed_path)
            
            # Both should have custom field
            assert meta1["custom"] == "data"
            assert meta2["custom"] == "data"
            # Timestamp should be different (or at least exist in both)
            assert "last_updated" in meta1
            assert "last_updated" in meta2
