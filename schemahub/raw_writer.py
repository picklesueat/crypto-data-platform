"""Utilities for writing raw records to disk."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping


def _default_serializer(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_jsonl(records: Iterable[Mapping], output_path: Path) -> None:
    """Write records to disk in JSON Lines format.

    This helper is intentionally minimal; Iceberg ingestion will eventually
    replace writing to local files.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as stream:
        for record in records:
            serialized = json.dumps(record, default=_default_serializer)
            stream.write(serialized + "\n")
