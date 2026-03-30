"""Tests for UUIDv7 generation."""

import time
import uuid

from data_assets.core.identifiers import uuid7


def test_uuid7_returns_valid_uuid():
    result = uuid7()
    assert isinstance(result, uuid.UUID)


def test_uuid7_version_is_7():
    result = uuid7()
    assert result.version == 7


def test_uuid7_uniqueness():
    ids = {uuid7() for _ in range(100)}
    assert len(ids) == 100


def test_uuid7_sortable_by_time():
    """UUIDv7s generated in sequence should sort chronologically."""
    first = uuid7()
    time.sleep(0.002)  # 2ms — enough for different millisecond timestamp
    second = uuid7()
    assert str(first) < str(second)


def test_uuid7_embeds_timestamp():
    """The first 48 bits should encode a recent Unix millisecond timestamp."""
    before_ms = int(time.time() * 1000)
    result = uuid7()
    after_ms = int(time.time() * 1000)

    # Extract the 48-bit timestamp from the UUID
    extracted_ms = result.int >> 80
    assert before_ms <= extracted_ms <= after_ms
