"""Tests for the in-process token-bucket rate limiter."""

import threading
import time

from data_assets.extract.rate_limiter import RateLimiter


def test_basic_acquire():
    limiter = RateLimiter(rate_per_second=100.0)
    start = time.monotonic()
    for _ in range(10):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 10 calls at 100/sec should be near-instant
    assert elapsed < 1.0


def test_rate_limiting_enforced():
    limiter = RateLimiter(rate_per_second=5.0)
    start = time.monotonic()
    for _ in range(6):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 6 tokens at 5/sec: first 5 instant, 6th needs ~0.2s
    assert elapsed >= 0.15


def test_thread_safety():
    """Multiple threads sharing a limiter should not exceed the rate."""
    # Rate of 10/sec with 4 threads x 5 calls = 20 calls total.
    # Bucket starts with 10 tokens, so first 10 are instant,
    # remaining 10 need ~1s to refill → total >= 0.8s
    limiter = RateLimiter(rate_per_second=10.0)
    call_count = 0
    lock = threading.Lock()

    def worker():
        nonlocal call_count
        for _ in range(5):
            limiter.acquire()
            with lock:
                call_count += 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    assert call_count == 20
    # 20 calls at 10/sec: first 10 instant, remaining 10 need ~1s
    assert elapsed >= 0.8


def test_pause_for_retry_after():
    limiter = RateLimiter(rate_per_second=100.0)
    limiter.pause_for(0.3)
    start = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.25
