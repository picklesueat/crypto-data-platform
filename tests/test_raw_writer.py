from datetime import datetime, timezone

import boto3
from botocore.stub import Stubber

from schemahub.raw_writer import write_jsonl_s3


def test_write_jsonl_s3_puts_object_payload():
    client = boto3.client("s3", region_name="us-east-1")
    stubber = Stubber(client)

    records = [
        {
            "trade_id": "1",
            "time": datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
            "price": 100.5,
        }
    ]

    expected_body = b'{"trade_id": "1", "time": "2024-06-01T12:00:00+00:00", "price": 100.5}\n'
    stubber.add_response(
        "put_object",
        {},
        {"Bucket": "my-bucket", "Key": "prefix/file.jsonl", "Body": expected_body},
    )

    with stubber:
        write_jsonl_s3(records, bucket="my-bucket", key="prefix/file.jsonl", s3_client=client)
