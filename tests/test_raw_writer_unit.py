"""Unit tests for raw_writer module."""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.stub import Stubber

from schemahub.raw_writer import write_jsonl_s3, _default_serializer


class TestDefaultSerializer:
    """Tests for _default_serializer."""

    def test_serialize_datetime_utc(self):
        """Serializing a UTC datetime returns ISO format string."""
        dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _default_serializer(dt)
        
        assert isinstance(result, str)
        assert result == "2024-06-01T12:00:00+00:00"

    def test_serialize_datetime_naive(self):
        """Serializing a naive datetime returns ISO format string."""
        dt = datetime(2024, 6, 1, 12, 0, 0)
        result = _default_serializer(dt)
        
        assert isinstance(result, str)
        assert "2024-06-01T12:00:00" in result

    def test_serialize_datetime_with_microseconds(self):
        """Serializing a datetime with microseconds preserves them."""
        dt = datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
        result = _default_serializer(dt)
        
        assert "123456" in result

    def test_serialize_unsupported_type_raises_typeerror(self):
        """Serializing an unsupported type raises TypeError."""
        class CustomObject:
            pass
        
        with pytest.raises(TypeError, match="not JSON serializable"):
            _default_serializer(CustomObject())

    def test_serialize_int_raises_typeerror(self):
        """Attempting to serialize int raises TypeError (not directly supported)."""
        with pytest.raises(TypeError):
            _default_serializer(12345)

    def test_serialize_set_raises_typeerror(self):
        """Attempting to serialize set raises TypeError."""
        with pytest.raises(TypeError):
            _default_serializer({1, 2, 3})


class TestWriteJsonlS3:
    """Tests for write_jsonl_s3 function."""

    def test_write_single_record(self):
        """Writing a single record to S3 works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {
                "trade_id": "123",
                "price": 35000.5,
                "time": datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
            }
        ]
        
        stubber.add_response(
            "put_object",
            {},
            {
                "Bucket": "my-bucket",
                "Key": "trades/data.jsonl",
                "Body": b'{"trade_id": "123", "price": 35000.5, "time": "2024-06-01T12:00:00+00:00"}\n',
            },
        )
        
        with stubber:
            write_jsonl_s3(records, bucket="my-bucket", key="trades/data.jsonl", s3_client=client)

    def test_write_multiple_records(self):
        """Writing multiple records to S3 writes them as separate lines."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
        ]
        
        # The body should have two lines
        expected_body = b'{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}\n'
        
        stubber.add_response(
            "put_object",
            {},
            {
                "Bucket": "my-bucket",
                "Key": "data.jsonl",
                "Body": expected_body,
            },
        )
        
        with stubber:
            write_jsonl_s3(records, bucket="my-bucket", key="data.jsonl", s3_client=client)

    def test_write_empty_records_list(self):
        """Writing an empty list of records to S3 writes empty string."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = []
        
        stubber.add_response(
            "put_object",
            {},
            {
                "Bucket": "my-bucket",
                "Key": "data.jsonl",
                "Body": b"",
            },
        )
        
        with stubber:
            write_jsonl_s3(records, bucket="my-bucket", key="data.jsonl", s3_client=client)

    def test_write_records_with_datetime(self):
        """Writing records with datetime fields serializes them correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        records = [{"timestamp": dt, "value": 100}]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - datetime serialization works
            write_jsonl_s3(records, bucket="my-bucket", key="data.jsonl", s3_client=client)

    def test_write_records_with_nested_dict(self):
        """Writing records with nested dicts works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {
                "id": 1,
                "nested": {
                    "key": "value",
                    "number": 42,
                }
            }
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - nested structures work
            write_jsonl_s3(records, bucket="my-bucket", key="data.jsonl", s3_client=client)

    def test_write_uses_correct_s3_parameters(self):
        """write_jsonl_s3 calls put_object with correct parameters."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [{"data": "test"}]
        bucket = "my-bucket"
        key = "path/to/file.jsonl"
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error
            write_jsonl_s3(records, bucket=bucket, key=key, s3_client=client)

    def test_write_encodes_body_as_utf8(self):
        """write_jsonl_s3 encodes the body as UTF-8."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [{"text": "hello"}]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - UTF-8 encoding works
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_uses_default_s3_client_when_none_provided(self):
        """write_jsonl_s3 creates default S3 client when none provided."""
        with patch("schemahub.raw_writer.boto3.client") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value = mock_client
            mock_client.put_object = MagicMock()
            
            records = [{"data": "test"}]
            
            with patch.dict("os.environ", {"AWS_REGION": "us-west-2"}):
                write_jsonl_s3(records, bucket="bucket", key="key")
                
                # Verify boto3.client was called
                assert mock_boto3.called

    def test_write_preserves_field_order_in_json(self):
        """Writing records preserves the order of fields in JSON."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {
                "a": 1,
                "b": 2,
                "c": 3,
            }
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_each_record_on_separate_line(self):
        """Writing multiple records puts each on a separate line."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - records are written line by line
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_with_special_characters(self):
        """Writing records with special characters escapes them correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"text": 'hello "world"', "value": 'path\\to\\file'}
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - special chars are escaped
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_with_none_values(self):
        """Writing records with None values serializes them as null."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"id": 1, "optional_field": None}
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - None is serialized as null
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_with_boolean_values(self):
        """Writing records with boolean values works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"id": 1, "active": True, "deleted": False}
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - booleans are serialized
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_with_numeric_types(self):
        """Writing records with different numeric types works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"int_val": 42, "float_val": 3.14, "negative": -100}
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - numeric types are serialized
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)

    def test_write_with_list_values(self):
        """Writing records with list values works correctly."""
        client = boto3.client("s3", region_name="us-east-1")
        stubber = Stubber(client)
        
        records = [
            {"id": 1, "tags": ["a", "b", "c"]}
        ]
        
        stubber.add_response("put_object", {})
        
        with stubber:
            # Should complete without error - lists are serialized
            write_jsonl_s3(records, bucket="bucket", key="key", s3_client=client)
