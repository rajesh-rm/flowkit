"""In-process token-bucket rate limiter (thread-safe)."""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token-bucket rate limiter.

    Shared across all threads in a parallel extraction so that
    the total outbound call rate is enforced globally.
    """

    def __init__(self, rate_per_second: float) -> None:
        self._rate = rate_per_second
        self._max_tokens = rate_per_second
        self._tokens = rate_per_second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._retry_after_until: float = 0.0

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            with self._lock:
                # Honour Retry-After pause
                now = time.monotonic()
                if now < self._retry_after_until:
                    wait = self._retry_after_until - now
                    self._lock.release()
                    time.sleep(wait)
                    self._lock.acquire()
                    now = time.monotonic()

                # Refill tokens
                elapsed = now - self._last_refill
                self._tokens = min(
                    self._max_tokens, self._tokens + elapsed * self._rate
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                # Calculate wait time for next token
                wait_time = (1.0 - self._tokens) / self._rate

            time.sleep(wait_time)

    def pause_for(self, seconds: float) -> None:
        """Pause all acquisitions for the given duration (Retry-After)."""
        with self._lock:
            deadline = time.monotonic() + seconds
            self._retry_after_until = max(self._retry_after_until, deadline)
            logger.info("Rate limiter paused for %.1f seconds (Retry-After)", seconds)
