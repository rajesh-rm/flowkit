"""UUIDv7 generation — timestamp-ordered, sortable UUIDs.

UUIDv7 embeds a Unix millisecond timestamp in the first 48 bits,
making UUIDs naturally sortable by creation time. The remaining
bits are random, preserving uniqueness guarantees.

Spec: RFC 9562, Section 5.7.
"""

from __future__ import annotations

import os
import time
import uuid


def uuid7() -> uuid.UUID:
    """Generate a UUIDv7 (timestamp-ordered, random)."""
    timestamp_ms = int(time.time() * 1000)
    rand_bytes = os.urandom(10)

    # Layout: 48-bit timestamp | 4-bit version=7 | 12-bit rand_a | 2-bit variant=10 | 62-bit rand_b
    uuid_int = (timestamp_ms & 0xFFFFFFFFFFFF) << 80
    uuid_int |= 0x7 << 76  # version 7
    uuid_int |= (int.from_bytes(rand_bytes[:2], "big") & 0x0FFF) << 64
    uuid_int |= 0x8 << 60  # variant 10xx
    uuid_int |= int.from_bytes(rand_bytes[2:], "big") & 0x0FFFFFFFFFFFFFFF

    return uuid.UUID(int=uuid_int)
