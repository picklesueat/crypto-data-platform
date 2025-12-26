"""Quick test script to verify transform/validation modules work together."""
import sys
import logging
from datetime import datetime, timezone

# Test imports
try:
    from schemahub.transform import load_mapping, transform_trade
    from schemahub.validation import validate_batch_and_check_manifest, check_data_quality_gates
    from schemahub.manifest import load_manifest, update_manifest_after_transform, should_trigger_replay
    print("✓ All module imports successful")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Test 1: Load mapping YAML
print("\n[Test 1] Loading mapping YAML...")
try:
    mapping = load_mapping("config/mappings/coinbase_transform.yaml")
    assert "field_mappings" in mapping
    assert "transformations" in mapping
    print(f"✓ Mapping loaded with {len(mapping['field_mappings'])} field mappings")
except Exception as e:
    print(f"✗ Failed to load mapping: {e}")
    sys.exit(1)

# Test 2: Transform a mock trade record
print("\n[Test 2] Transforming mock trade...")
try:
    mock_trade = {
        "id": "12345",
        "product_id": "BTC-USD",
        "side": "BUY",
        "price": "43500.50",
        "size": "0.5",
        "time": "2025-12-25T10:30:00Z",
    }
    unified = transform_trade(mock_trade, mapping)
    assert unified is not None
    assert unified["exchange"] == "coinbase"
    assert unified["symbol"] == "BTC-USD"
    assert unified["trade_id"] == "12345"
    assert unified["side"] == "buy"  # Should be lowercase
    print(f"✓ Trade transformed successfully: {unified['symbol']} {unified['side']} {unified['quantity']} @ ${unified['price']}")
except Exception as e:
    print(f"✗ Trade transformation failed: {e}")
    sys.exit(1)

# Test 3: Test validation functions (mock data)
print("\n[Test 3] Testing validation functions...")
try:
    # Create mock batch metrics
    batch_issues = []
    batch_metrics = {
        "batch_records_checked": 100,
        "duplicates_found": 0,
        "schema_errors": 0,
        "stale_products": [],
    }
    
    # Test quality gate check
    gate_passed, reasons = check_data_quality_gates(batch_issues, batch_metrics)
    assert gate_passed == True
    assert len(reasons) == 0
    print(f"✓ Quality gates check: {gate_passed} (no issues)")
except Exception as e:
    print(f"✗ Validation function test failed: {e}")
    sys.exit(1)

# Test 4: Test manifest functions
print("\n[Test 4] Testing manifest functions...")
try:
    # Create a mock manifest
    manifest = {
        "processed_raw_files": [],
        "product_stats": {},
        "transform_history": [],
        "health": {
            "last_successful_transform": None,
            "last_validation_issues": [],
            "consecutive_failures": 0,
        },
        "dup_trends": [],
        "last_version": 1,
        "last_update_ts": None,
    }
    
    # Test update_manifest_after_transform
    transform_result = {
        "records_read": 100,
        "records_transformed": 100,
        "records_written": 100,
        "status": "success",
        "output_version": 1,
        "s3_key": "s3://bucket/v1/unified_trades_2025.parquet",
    }
    
    updated_manifest = update_manifest_after_transform(
        bucket="mock-bucket",
        manifest=manifest,
        transform_result=transform_result,
        batch_issues=[],
        batch_metrics=batch_metrics,
        quality_gate_passed=True,
    )
    
    assert len(updated_manifest["transform_history"]) == 1
    assert updated_manifest["health"]["consecutive_failures"] == 0
    print(f"✓ Manifest updated: {len(updated_manifest['transform_history'])} transforms tracked")
    
    # Test should_trigger_replay
    should_replay, reason = should_trigger_replay(updated_manifest)
    assert should_replay == False
    print(f"✓ Replay check: should_replay={should_replay}")
    
except Exception as e:
    print(f"✗ Manifest test failed: {e}")
    sys.exit(1)

print("\n" + "="*50)
print("All tests passed! ✓")
print("="*50)
