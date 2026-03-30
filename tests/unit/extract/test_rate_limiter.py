"""Tests for the sliding-window rate limiter."""

import threading
import time

from data_assets.extract.rate_limiter import RateLimiter


def test_basic_acquire():
    """Calls within the rate limit should return immediately."""
    limiter = RateLimiter(rate_per_second=100.0)
    start = time.monotonic()
    for _ in range(10):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 10 calls at 100/sec — well within window
    assert elapsed < 1.0


def test_rate_limiting_enforced():
    """6th call at 5/sec must block until the window has room."""
    limiter = RateLimiter(rate_per_second=5.0)
    start = time.monotonic()
    for _ in range(6):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # First 5 instant (fill the 1-sec window), 6th waits for oldest to expire
    assert elapsed >= 0.15


def test_thread_safety():
    """4 threads × 5 calls at 10/sec = 20 calls, must take ~1s for the last 10."""
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
    # First 10 calls fill the window instantly, remaining 10 need ~1s
    assert elapsed >= 0.8


def test_pause_for_retry_after():
    """pause_for() blocks all acquire() calls for the specified duration."""
    limiter = RateLimiter(rate_per_second=100.0)
    limiter.pause_for(0.3)
    start = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.25


def test_window_resets_after_time():
    """After the window passes, slots become available again."""
    limiter = RateLimiter(rate_per_second=5.0)
    # Fill the window
    for _ in range(5):
        limiter.acquire()
    # Wait for window to clear
    time.sleep(1.1)
    # Should be instant again
    start = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.1
