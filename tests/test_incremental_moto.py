import json
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from schemahub.manifest import load_manifest, update_manifest_after_transform
from schemahub.transform import transform_raw_to_unified


def _put_jsonl(s3_client, bucket: str, key: str, records: list[dict]):
    body = "".join(json.dumps(r) + "\n" for r in records)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


@mock_aws
def test_incremental_transform_skips_processed_files():
    region = "us-east-1"
    bucket = "test-bucket"
    raw_prefix = "schemahub/raw_coinbase_trades/test"
    unified_prefix = "schemahub/unified_trades/test"
    manifest_key = "schemahub/manifest_test.json"

    s3 = boto3.client("s3", region_name=region)
    s3.create_bucket(Bucket=bucket)

    # Create two initial raw files (A, B)
    records_a = [
        {
            "id": "A-1",
            "product_id": "BTC-USD",
            "side": "BUY",
            "price": "100",
            "size": "0.01",
            "time": datetime.now(timezone.utc).isoformat(),
        }
    ]
    records_b = [
        {
            "id": "B-1",
            "product_id": "ETH-USD",
            "side": "SELL",
            "price": "2000",
            "size": "0.5",
            "time": datetime.now(timezone.utc).isoformat(),
        }
    ]

    key_a = f"{raw_prefix}/raw_a.jsonl"
    key_b = f"{raw_prefix}/raw_b.jsonl"
    _put_jsonl(s3, bucket, key_a, records_a)
    _put_jsonl(s3, bucket, key_b, records_b)

    # First run (incremental, no manifest yet) should process A+B
    result1 = transform_raw_to_unified(
        bucket=bucket,
        raw_prefix=raw_prefix,
        unified_prefix=unified_prefix,
        version=1,
        run_id="run-1",
        rebuild=False,
        manifest_key=manifest_key,
    )

    manifest = load_manifest(bucket, manifest_key)
    manifest = update_manifest_after_transform(
        bucket=bucket,
        manifest=manifest,
        transform_result=result1,
        batch_issues=[],
        batch_metrics={},
        quality_gate_passed=True,
        manifest_key=manifest_key,
    )

    assert result1["records_read"] == 2
    assert len(manifest["processed_raw_files"]) == 2
    assert set(manifest["processed_raw_files"]) == {key_a, key_b}

    # Add new raw file C
    records_c = [
        {
            "id": "C-1",
            "product_id": "BTC-USD",
            "side": "BUY",
            "price": "150",
            "size": "0.02",
            "time": datetime.now(timezone.utc).isoformat(),
        }
    ]
    key_c = f"{raw_prefix}/raw_c.jsonl"
    _put_jsonl(s3, bucket, key_c, records_c)

    # Second run should process only C (skip A,B)
    result2 = transform_raw_to_unified(
        bucket=bucket,
        raw_prefix=raw_prefix,
        unified_prefix=unified_prefix,
        version=1,
        run_id="run-2",
        rebuild=False,
        manifest_key=manifest_key,
    )

    manifest = load_manifest(bucket, manifest_key)
    manifest = update_manifest_after_transform(
        bucket=bucket,
        manifest=manifest,
        transform_result=result2,
        batch_issues=[],
        batch_metrics={},
        quality_gate_passed=True,
        manifest_key=manifest_key,
    )

    assert result2["records_read"] == 1
    assert len(manifest["processed_raw_files"]) == 3
    assert key_c in manifest["processed_raw_files"]

    # Ensure no duplication from reprocessing
    assert manifest["processed_raw_files"].count(key_a) == 1
    assert manifest["processed_raw_files"].count(key_b) == 1
    assert manifest["processed_raw_files"].count(key_c) == 1


@mock_aws
def test_rebuild_processes_all_files_even_if_manifest_has_entries():
    region = "us-east-1"
    bucket = "test-bucket"
    raw_prefix = "schemahub/raw_coinbase_trades/test-rebuild"
    unified_prefix = "schemahub/unified_trades/test-rebuild"
    manifest_key = "schemahub/manifest_test_rebuild.json"

    s3 = boto3.client("s3", region_name=region)
    s3.create_bucket(Bucket=bucket)

    # Existing manifest that claims file_a already processed
    manifest = {
        "processed_raw_files": [f"{raw_prefix}/raw_a.jsonl"],
        "product_stats": {},
        "transform_history": [],
        "health": {"last_successful_transform": None, "last_validation_issues": [], "consecutive_failures": 0},
        "dup_trends": [],
        "last_version": 1,
        "last_update_ts": None,
        "replayed_versions": {},
    }
    # Save manifest
    s3.put_object(Bucket=bucket, Key=manifest_key, Body=json.dumps(manifest).encode("utf-8"))

    # Upload two files A/B
    records_a = [{"id": "A-1", "product_id": "BTC-USD", "side": "BUY", "price": "100", "size": "0.01", "time": datetime.now(timezone.utc).isoformat()}]
    records_b = [{"id": "B-1", "product_id": "ETH-USD", "side": "SELL", "price": "2000", "size": "0.5", "time": datetime.now(timezone.utc).isoformat()}]
    key_a = f"{raw_prefix}/raw_a.jsonl"
    key_b = f"{raw_prefix}/raw_b.jsonl"
    _put_jsonl(s3, bucket, key_a, records_a)
    _put_jsonl(s3, bucket, key_b, records_b)

    # Rebuild should ignore manifest skip list and process both files
    result = transform_raw_to_unified(
        bucket=bucket,
        raw_prefix=raw_prefix,
        unified_prefix=unified_prefix,
        version=1,
        run_id="run-rebuild",
        rebuild=True,
        manifest_key=manifest_key,
    )

    assert result["records_read"] == 2
    assert set(result["processed_files"]) == {key_a, key_b}


@mock_aws
def test_missing_manifest_falls_back_to_full_refresh():
    region = "us-east-1"
    bucket = "test-bucket"
    raw_prefix = "schemahub/raw_coinbase_trades/test-nomanifest"
    unified_prefix = "schemahub/unified_trades/test-nomanifest"
    manifest_key = "schemahub/manifest_does_not_exist.json"

    s3 = boto3.client("s3", region_name=region)
    s3.create_bucket(Bucket=bucket)

    # Upload two files A/B
    records_a = [{"id": "A-1", "product_id": "BTC-USD", "side": "BUY", "price": "100", "size": "0.01", "time": datetime.now(timezone.utc).isoformat()}]
    records_b = [{"id": "B-1", "product_id": "ETH-USD", "side": "SELL", "price": "2000", "size": "0.5", "time": datetime.now(timezone.utc).isoformat()}]
    key_a = f"{raw_prefix}/raw_a.jsonl"
    key_b = f"{raw_prefix}/raw_b.jsonl"
    _put_jsonl(s3, bucket, key_a, records_a)
    _put_jsonl(s3, bucket, key_b, records_b)

    # No manifest exists; transform should process all
    result = transform_raw_to_unified(
        bucket=bucket,
        raw_prefix=raw_prefix,
        unified_prefix=unified_prefix,
        version=1,
        run_id="run-nomanifest",
        rebuild=False,
        manifest_key=manifest_key,
    )

    assert result["records_read"] == 2
    assert set(result["processed_files"]) == {key_a, key_b}
