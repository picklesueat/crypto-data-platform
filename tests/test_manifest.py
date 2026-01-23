"""Unit tests for manifest management functions."""
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

import boto3
from moto import mock_aws

from schemahub.manifest import (
    load_manifest,
    save_manifest,
    update_manifest_after_transform,
    should_trigger_replay,
    get_next_version,
    mark_replay,
    MANIFEST_KEY,
)


@pytest.fixture
def s3_bucket():
    """Create a mocked S3 bucket for testing."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)
        yield s3, bucket


# ============================================================================
# load_manifest tests
# ============================================================================

class TestLoadManifest:
    """Tests for load_manifest function."""

    def test_loads_existing_manifest(self, s3_bucket):
        """Test loading an existing manifest from S3."""
        s3, bucket = s3_bucket

        manifest_data = {
            "processed_raw_files": ["file1.json", "file2.json"],
            "product_stats": {"BTC-USD": {"last_trade_id": 1000}},
            "transform_history": [],
            "health": {
                "last_successful_transform": "2024-01-01T00:00:00Z",
                "last_validation_issues": [],
                "consecutive_failures": 0,
            },
            "dup_trends": [],
            "last_version": 1,
            "last_update_ts": "2024-01-01T00:00:00Z",
        }
        s3.put_object(
            Bucket=bucket,
            Key=MANIFEST_KEY,
            Body=json.dumps(manifest_data),
        )

        result = load_manifest(bucket)

        assert result["processed_raw_files"] == ["file1.json", "file2.json"]
        assert result["product_stats"]["BTC-USD"]["last_trade_id"] == 1000
        assert result["last_version"] == 1

    def test_returns_default_when_manifest_not_found(self, s3_bucket):
        """Test returning default manifest when file doesn't exist."""
        s3, bucket = s3_bucket

        result = load_manifest(bucket)

        assert result["processed_raw_files"] == []
        assert result["product_stats"] == {}
        assert result["transform_history"] == []
        assert result["health"]["consecutive_failures"] == 0
        assert result["last_version"] == 1

    def test_returns_default_on_json_parse_error(self, s3_bucket):
        """Test returning default manifest on JSON parse error."""
        s3, bucket = s3_bucket

        s3.put_object(
            Bucket=bucket,
            Key=MANIFEST_KEY,
            Body="invalid json {{{",
        )

        result = load_manifest(bucket)

        # Should return default structure
        assert result["processed_raw_files"] == []
        assert result["last_version"] == 1

    def test_uses_custom_manifest_key(self, s3_bucket):
        """Test loading from custom manifest key."""
        s3, bucket = s3_bucket

        custom_key = "custom/manifest.json"
        manifest_data = {"last_version": 2}
        s3.put_object(
            Bucket=bucket,
            Key=custom_key,
            Body=json.dumps(manifest_data),
        )

        result = load_manifest(bucket, manifest_key=custom_key)

        assert result["last_version"] == 2


# ============================================================================
# save_manifest tests
# ============================================================================

class TestSaveManifest:
    """Tests for save_manifest function."""

    def test_saves_manifest_to_s3(self, s3_bucket):
        """Test saving manifest to S3."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": ["file1.json"],
            "last_version": 1,
            "last_update_ts": "2024-01-01T00:00:00Z",
        }

        result = save_manifest(bucket, manifest)

        assert result is True

        # Verify it was saved correctly
        response = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        saved_manifest = json.loads(response["Body"].read().decode("utf-8"))
        assert saved_manifest["last_version"] == 1
        assert saved_manifest["processed_raw_files"] == ["file1.json"]

    def test_saves_with_custom_key(self, s3_bucket):
        """Test saving manifest with custom key."""
        s3, bucket = s3_bucket

        custom_key = "custom/manifest.json"
        manifest = {"last_version": 2}

        result = save_manifest(bucket, manifest, manifest_key=custom_key)

        assert result is True

        # Verify it was saved to custom key
        response = s3.get_object(Bucket=bucket, Key=custom_key)
        saved_manifest = json.loads(response["Body"].read().decode("utf-8"))
        assert saved_manifest["last_version"] == 2

    def test_handles_datetime_serialization(self, s3_bucket):
        """Test that datetime objects are serialized correctly."""
        s3, bucket = s3_bucket

        manifest = {
            "last_update_ts": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        }

        result = save_manifest(bucket, manifest)

        assert result is True

        # Verify datetime was serialized
        response = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        saved_manifest = json.loads(response["Body"].read().decode("utf-8"))
        assert "2024-01-01" in saved_manifest["last_update_ts"]

    def test_returns_false_on_error(self, s3_bucket):
        """Test returning False when save fails."""
        s3, bucket = s3_bucket

        manifest = {"last_version": 1}

        # Use non-existent bucket to cause error
        result = save_manifest("nonexistent-bucket", manifest)

        assert result is False


# ============================================================================
# update_manifest_after_transform tests
# ============================================================================

class TestUpdateManifestAfterTransform:
    """Tests for update_manifest_after_transform function."""

    def test_adds_processed_files(self, s3_bucket):
        """Test that processed files are added to manifest."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }
        transform_result = {
            "processed_files": ["raw/file1.json", "raw/file2.json"],
            "records_read": 100,
            "records_transformed": 100,
            "records_written": 100,
            "status": "success",
        }
        batch_issues = []
        batch_metrics = {"batch_records_checked": 100, "duplicates_found": 0}

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result=transform_result,
            batch_issues=batch_issues,
            batch_metrics=batch_metrics,
            quality_gate_passed=True,
        )

        assert "raw/file1.json" in result["processed_raw_files"]
        assert "raw/file2.json" in result["processed_raw_files"]

    def test_adds_transform_history_entry(self, s3_bucket):
        """Test that transform history entry is added."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }
        transform_result = {
            "records_read": 100,
            "records_transformed": 95,
            "records_written": 95,
            "status": "success",
            "output_version": 1,
            "s3_key": "unified/v1/output.parquet",
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result=transform_result,
            batch_issues=[],
            batch_metrics={"batch_records_checked": 95, "duplicates_found": 0},
            quality_gate_passed=True,
        )

        assert len(result["transform_history"]) == 1
        entry = result["transform_history"][0]
        assert entry["records_read"] == 100
        assert entry["records_transformed"] == 95
        assert entry["quality_gate_passed"] is True

    def test_resets_consecutive_failures_on_success(self, s3_bucket):
        """Test that consecutive_failures is reset on quality gate pass."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 3},
            "dup_trends": [],
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "success"},
            batch_issues=[],
            batch_metrics={},
            quality_gate_passed=True,
        )

        assert result["health"]["consecutive_failures"] == 0
        assert result["health"]["last_successful_transform"] is not None

    def test_increments_consecutive_failures_on_failure(self, s3_bucket):
        """Test that consecutive_failures is incremented on quality gate fail."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 1},
            "dup_trends": [],
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "failed"},
            batch_issues=["Schema error"],
            batch_metrics={},
            quality_gate_passed=False,
        )

        assert result["health"]["consecutive_failures"] == 2
        assert result["health"]["last_validation_issues"] == ["Schema error"]

    def test_triggers_replay_after_two_consecutive_failures(self, s3_bucket):
        """Test that replay is triggered after 2 consecutive failures."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 1},
            "dup_trends": [],
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "failed"},
            batch_issues=["Error"],
            batch_metrics={},
            quality_gate_passed=False,
        )

        assert result["health"]["consecutive_failures"] == 2
        assert result.get("_replay_triggered") is True

    def test_tracks_duplicate_trends(self, s3_bucket):
        """Test that duplicate trends are tracked."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "success"},
            batch_issues=[],
            batch_metrics={"batch_records_checked": 100, "duplicates_found": 5},
            quality_gate_passed=True,
        )

        assert len(result["dup_trends"]) == 1
        assert result["dup_trends"][0]["duplicates_found"] == 5
        assert result["dup_trends"][0]["batch_size"] == 100

    def test_updates_last_update_ts(self, s3_bucket):
        """Test that last_update_ts is updated."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
            "last_update_ts": None,
        }

        result = update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "success"},
            batch_issues=[],
            batch_metrics={},
            quality_gate_passed=True,
        )

        assert result["last_update_ts"] is not None

    def test_saves_to_s3(self, s3_bucket):
        """Test that manifest is saved to S3 after update."""
        s3, bucket = s3_bucket

        manifest = {
            "processed_raw_files": [],
            "product_stats": {},
            "transform_history": [],
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }

        update_manifest_after_transform(
            bucket=bucket,
            manifest=manifest,
            transform_result={"status": "success"},
            batch_issues=[],
            batch_metrics={},
            quality_gate_passed=True,
        )

        # Verify it was saved
        response = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        saved = json.loads(response["Body"].read().decode("utf-8"))
        assert saved["last_update_ts"] is not None


# ============================================================================
# should_trigger_replay tests
# ============================================================================

class TestShouldTriggerReplay:
    """Tests for should_trigger_replay function."""

    def test_returns_true_when_replay_flag_set(self):
        """Test returns True when _replay_triggered flag is set."""
        manifest = {
            "_replay_triggered": True,
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }

        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is True
        assert "replay flag" in reason.lower()

    def test_returns_true_on_consecutive_failures(self):
        """Test returns True when consecutive_failures >= 2."""
        manifest = {
            "health": {"consecutive_failures": 2},
            "dup_trends": [],
        }

        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is True
        assert "consecutive failures" in reason.lower()

    def test_returns_true_on_high_duplicate_ratio(self):
        """Test returns True when duplicate ratio > 5%."""
        manifest = {
            "health": {"consecutive_failures": 0},
            "dup_trends": [
                {"duplicates_found": 10, "batch_size": 100},  # 10% duplicates
            ],
        }

        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is True
        assert "duplicate ratio" in reason.lower()

    def test_returns_false_when_no_issues(self):
        """Test returns False when no issues detected."""
        manifest = {
            "health": {"consecutive_failures": 0},
            "dup_trends": [
                {"duplicates_found": 2, "batch_size": 100},  # 2% duplicates - OK
            ],
        }

        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is False
        assert reason == ""

    def test_handles_empty_dup_trends(self):
        """Test handles empty dup_trends gracefully."""
        manifest = {
            "health": {"consecutive_failures": 0},
            "dup_trends": [],
        }

        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is False

    def test_handles_zero_batch_size(self):
        """Test handles zero batch_size without division error."""
        manifest = {
            "health": {"consecutive_failures": 0},
            "dup_trends": [
                {"duplicates_found": 5, "batch_size": 0},
            ],
        }

        # Should not raise division by zero
        should_replay, reason = should_trigger_replay(manifest)

        assert should_replay is False


# ============================================================================
# get_next_version tests
# ============================================================================

class TestGetNextVersion:
    """Tests for get_next_version function."""

    def test_returns_2_when_current_is_1(self):
        """Test returns version 2 when current version is 1."""
        manifest = {"last_version": 1}

        result = get_next_version(manifest)

        assert result == 2

    def test_returns_1_when_current_is_2(self):
        """Test returns version 1 when current version is 2."""
        manifest = {"last_version": 2}

        result = get_next_version(manifest)

        assert result == 1

    def test_defaults_to_1_when_missing(self):
        """Test defaults to version 1 when last_version is missing."""
        manifest = {}

        result = get_next_version(manifest)

        # Default is 1, so next should be 2
        assert result == 2


# ============================================================================
# mark_replay tests
# ============================================================================

class TestMarkReplay:
    """Tests for mark_replay function."""

    def test_records_replay_event(self):
        """Test that replay event is recorded."""
        manifest = {}

        result = mark_replay(manifest, old_version=1, new_version=2, reason="High duplicates")

        assert "replayed_versions" in result
        assert "1_to_2" in result["replayed_versions"]
        assert len(result["replayed_versions"]["1_to_2"]) == 1
        assert result["replayed_versions"]["1_to_2"][0]["reason"] == "High duplicates"

    def test_appends_to_existing_replay_history(self):
        """Test that replay events are appended to existing history."""
        manifest = {
            "replayed_versions": {
                "1_to_2": [{"timestamp": "2024-01-01T00:00:00Z", "reason": "First replay"}]
            }
        }

        result = mark_replay(manifest, old_version=1, new_version=2, reason="Second replay")

        assert len(result["replayed_versions"]["1_to_2"]) == 2
        assert result["replayed_versions"]["1_to_2"][1]["reason"] == "Second replay"

    def test_handles_2_to_1_transition(self):
        """Test recording v2 to v1 transition."""
        manifest = {}

        result = mark_replay(manifest, old_version=2, new_version=1, reason="Rollback")

        assert "2_to_1" in result["replayed_versions"]
        assert result["replayed_versions"]["2_to_1"][0]["reason"] == "Rollback"

    def test_includes_timestamp(self):
        """Test that timestamp is included in replay record."""
        manifest = {}

        result = mark_replay(manifest, old_version=1, new_version=2, reason="Test")

        timestamp = result["replayed_versions"]["1_to_2"][0]["timestamp"]
        assert timestamp is not None
        assert "Z" in timestamp  # Should be ISO format with Z suffix
